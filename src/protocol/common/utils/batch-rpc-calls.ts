import AsyncLock from "async-lock";
import { ethers } from "ethers";
import { backOff } from "exponential-backoff";
import { zipWith } from "lodash";
import * as Rx from "rxjs";
import { RpcCallMethod, RpcConfig } from "../../../types/rpc-config";
import { MIN_DELAY_BETWEEN_RPC_CALLS_MS } from "../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../utils/rpc/archive-node-needed";
import { bufferUntilCountReached } from "../../../utils/rxjs/utils/buffer-until-count-reached";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ImportCtx } from "../types/import-context";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { getRpcRetryConfig } from "./rpc-retry-config";

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

export function batchRpcCalls$<TObj, TRes, TQueryObj, TQueryResp>(options: {
  ctx: ImportCtx<TObj>;
  getQuery: (obj: TObj) => TQueryObj;
  processBatch: (provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider, queryObjs: TQueryObj[]) => Promise<TQueryResp[]>;
  // we are doing this much rpc calls per input object
  // this is used to calculate the input batch to send to the client
  // and to know if we can inject the batch provider or if we should use the regular provider
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  getCallMultiplierForObj?: (obj: TObj) => number;
  logInfos: { msg: string; data?: Record<string, unknown> };
  formatOutput: (objs: TObj, results: TQueryResp) => TRes;
}) {
  const retryConfig = getRpcRetryConfig({ logInfos: options.logInfos, maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs });

  // get the rpc provider maximum batch size
  const methodLimitations = options.ctx.rpcConfig.limitations;

  // find out the max number of objects to process in a batch
  let canUseBatchProvider = true;
  let maxInputObjsPerBatch = options.ctx.streamConfig.maxInputTake;
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
    if (MIN_DELAY_BETWEEN_RPC_CALLS_MS[options.ctx.rpcConfig.chain] === "no-limit") {
      maxInputObjsPerBatch = Math.max(1, Math.floor(options.ctx.streamConfig.maxInputTake / 0.1));
    } else {
      maxInputObjsPerBatch = 1;
    }
  }
  logger.trace(
    mergeLogsInfos({ msg: "batchRpcCalls$ config", data: { maxInputObjsPerBatch, canUseBatchProvider, methodLimitations } }, options.logInfos),
  );

  return Rx.pipe(
    // take a batch of items
    bufferUntilCountReached<TObj>({
      maxBufferSize: maxInputObjsPerBatch,
      maxBufferTimeMs: options.ctx.streamConfig.maxInputWaitMs,
      pollFrequencyMs: 150,
      pollJitterMs: 50,
      logInfos: options.logInfos,
      getCount: options.getCallMultiplierForObj || (() => 1),
    }),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs: TObj[]) => {
      logger.trace(mergeLogsInfos({ msg: "batchRpcCalls$ - batch", data: { objsCount: objs.length } }, options.logInfos));

      const contractCalls = objs.map((obj) => options.getQuery(obj));

      const provider = canUseBatchProvider ? options.ctx.rpcConfig.batchProvider : options.ctx.rpcConfig.linearProvider;

      const work = async () => {
        logger.trace(mergeLogsInfos({ msg: "Ready to call RPC", data: { chain: options.ctx.rpcConfig.chain } }, options.logInfos));

        try {
          const res = await options.processBatch(provider, contractCalls);
          return res;
        } catch (error) {
          if (error instanceof ArchiveNodeNeededError) {
            throw error;
          } else if (isErrorDueToMissingDataFromNode(error)) {
            throw new ArchiveNodeNeededError(options.ctx.rpcConfig.chain, error);
          }
          throw error;
        }
      };

      try {
        // retry the call if it fails
        const responses = await backOff(() => {
          // acquire a lock for this chain so in case we do use batch provider, we are guaranteed
          // the we are only batching the current request size and not more
          logger.trace(mergeLogsInfos({ msg: "Acquiring provider lock for chain", data: { chain: options.ctx.rpcConfig.chain } }, options.logInfos));

          return callLockProtectedRpc(options.ctx.rpcConfig.chain, provider, options.ctx.rpcConfig.limitations, () =>
            chainLock.acquire(options.ctx.rpcConfig.chain, work),
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
        logger.error(mergeLogsInfos({ msg: "Error doing batch rpc work", data: { err } }, options.logInfos));
        logger.error(err);
        for (const obj of objs) {
          options.ctx.emitErrors(obj);
        }
        return Rx.EMPTY;
      }
    }, options.ctx.streamConfig.workConcurrency),

    Rx.tap(
      (objs) =>
        Array.isArray(objs) && logger.trace(mergeLogsInfos({ msg: "batchRpcCalls$ - done", data: { objsCount: objs.length } }, options.logInfos)),
    ),

    // flatten
    Rx.mergeAll(),
  );
}
