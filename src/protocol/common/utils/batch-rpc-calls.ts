import * as Rx from "rxjs";
import { backOff } from "exponential-backoff";
import { zipWith } from "lodash";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { rootLogger } from "../../../utils/logger";
import { DbProduct } from "../loader/product";
import { getRpcRetryConfig } from "./rpc-retry-config";
import { RpcCallMethod, RpcConfig } from "../../../types/rpc-config";
import AsyncLock from "async-lock";
import { ethers } from "ethers";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../utils/rpc/archive-node-needed";
import { bufferUntilAccumulatedCountReached } from "../../../utils/rxjs/utils/buffer-until-accumulated-count";
import { MIN_DELAY_BETWEEN_RPC_CALLS_MS } from "../../../utils/config";

const logger = rootLogger.child({ module: "utils", component: "batch-rpc-calls" });

export interface BatchStreamConfig {
  // how many items to take from the input stream before making groups
  maxInputTake: number;
  // how long to wait before making groups
  maxInputWaitMs: number;

  // how many concurrent groups to process at the same time
  workConcurrency: number;

  // how long at most to attempt retries
  maxTotalRetryMs: number;
}

const chainLock = new AsyncLock({
  // max amount of time an item can remain in the queue before acquiring the lock
  timeout: 0, // never
  // we don't want a lock to be reentered
  domainReentrant: false,
  //max amount of time allowed between entering the queue and completing execution
  maxOccupationTime: 0, // never
  // max number of tasks allowed in the queue at a time
  maxPending: 100_000,
});

export function batchRpcCalls$<TTarget, TInputObj extends ImportQuery<TTarget>, TQueryObj, TResp, TRes extends ImportQuery<TTarget>>(options: {
  getQuery: (obj: TInputObj) => TQueryObj;
  processBatch: (provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider, queryObjs: TQueryObj[]) => Promise<TResp[]>;
  // we are doing this much rpc calls per input object
  // this is used to calculate the input batch to send to the client
  // and to know if we can inject the batch provider or if we should use the regular provider
  rpcConfig: RpcConfig;
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  getCallMultiplierForObj?: (obj: TInputObj) => number;
  // errors
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter<TTarget>;
  logInfos: { msg: string; data?: Record<string, unknown> };
  // output
  formatOutput: (objs: TInputObj, results: TResp) => TRes;
}) {
  const retryConfig = getRpcRetryConfig({ logInfos: options.logInfos, maxTotalRetryMs: options.streamConfig.maxTotalRetryMs });

  // get the rpc provider maximum batch size
  const methodLimitations = options.rpcConfig.limitations;

  // find out the max number of objects to process in a batch
  let canUseBatchProvider = true;
  let maxInputObjsPerBatch = options.streamConfig.maxInputTake;
  for (const [method, count] of Object.entries(options.rpcCallsPerInputObj)) {
    // we don't use this method so we don't care
    if (count <= 0) {
      continue;
    }
    const maxCount = methodLimitations[method as RpcCallMethod];
    if (maxCount === null) {
      canUseBatchProvider = false;
      break;
    }
    maxInputObjsPerBatch = Math.min(maxInputObjsPerBatch, Math.floor(maxCount / count));
  }

  if (!canUseBatchProvider) {
    // do some amount of concurrent rpc calls for RPCs without rate limiting but without batch provider active
    if (MIN_DELAY_BETWEEN_RPC_CALLS_MS[options.rpcConfig.chain] === "no-limit") {
      maxInputObjsPerBatch = Math.max(1, Math.floor(options.streamConfig.maxInputTake / 0.1));
    } else {
      maxInputObjsPerBatch = 1;
    }
  }
  logger.trace({
    msg: "batchRpcCalls$ config. " + options.logInfos.msg,
    data: { maxInputObjsPerBatch, canUseBatchProvider, methodLimitations, ...options.logInfos.data },
  });

  return Rx.pipe(
    // take a batch of items
    bufferUntilAccumulatedCountReached<TInputObj>({
      maxBufferSize: maxInputObjsPerBatch,
      maxBufferTimeMs: options.streamConfig.maxInputWaitMs,
      pollFrequencyMs: 150,
      pollJitterMs: 50,
      logInfos: options.logInfos,
      getCount: options.getCallMultiplierForObj || (() => 1),
    }),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs: TInputObj[]) => {
      logger.trace({ msg: "batchRpcCalls$ - batch. " + options.logInfos.msg, data: { ...options.logInfos.data, objsCount: objs.length } });

      const contractCalls = objs.map((obj) => options.getQuery(obj));

      const provider = canUseBatchProvider ? options.rpcConfig.batchProvider : options.rpcConfig.linearProvider;

      const work = async () => {
        logger.trace({
          msg: "Ready to call RPC. " + options.logInfos.msg,
          data: { chain: options.rpcConfig.chain, ...options.logInfos.data },
        });

        try {
          const res = await options.processBatch(provider, contractCalls);
          return res;
        } catch (error) {
          if (error instanceof ArchiveNodeNeededError) {
            throw error;
          } else if (isErrorDueToMissingDataFromNode(error)) {
            throw new ArchiveNodeNeededError(options.rpcConfig.chain, error);
          }
          throw error;
        }
      };

      try {
        // retry the call if it fails
        const responses = await backOff(() => {
          // acquire a lock for this chain so in case we do use batch provider, we are guaranteed
          // the we are only batching the current request size and not more
          logger.trace({
            msg: "Acquiring provider lock for chain. " + options.logInfos.msg,
            data: { chain: options.rpcConfig.chain, ...options.logInfos.data },
          });

          return callLockProtectedRpc(options.rpcConfig.chain, provider, options.rpcConfig.limitations, () =>
            chainLock.acquire(options.rpcConfig.chain, work),
          );
        }, retryConfig);

        if (responses.length !== objs.length) {
          throw new ProgrammerError({
            msg: "Query and result length mismatch",
            data: { contractCallsCount: contractCalls.length, responsesLength: responses.length },
          });
        }
        return zipWith(objs, responses, options.formatOutput);
      } catch (err) {
        // here, none of the retrying worked, so we emit all the objects as in error
        logger.error({ msg: "Error doing batch rpc work: " + options.logInfos.msg, data: { err, ...options.logInfos.data } });
        logger.error(err);
        for (const obj of objs) {
          options.emitErrors(obj);
        }
        return Rx.EMPTY;
      }
    }, options.streamConfig.workConcurrency),

    Rx.tap(
      (objs) =>
        Array.isArray(objs) &&
        logger.trace({ msg: "batchRpcCalls$ - done. " + options.logInfos.msg, data: { ...options.logInfos.data, objsCount: objs.length } }),
    ),

    // flatten
    Rx.mergeAll(),
  );
}
