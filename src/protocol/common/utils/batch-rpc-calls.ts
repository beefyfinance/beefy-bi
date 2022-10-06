import * as Rx from "rxjs";
import { backOff } from "exponential-backoff";
import { zipWith } from "lodash";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { rootLogger } from "../../../utils/logger";
import { DbProduct } from "../loader/product";
import { getRpcRetryConfig } from "./rpc-retry-config";
import { RpcCallMethod, RpcConfig } from "../../../types/rpc-config";
import AsyncLock from "async-lock";
import { ethers } from "ethers";

const logger = rootLogger.child({ module: "utils", component: "batch-query-group" });

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

export function batchRpcCalls$<
  TProduct extends DbProduct,
  TInputObj extends ProductImportQuery<TProduct>,
  TQueryObj,
  TResp,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  rpcConfig: RpcConfig;
  getQuery: (obj: TInputObj) => TQueryObj;
  processBatch: (provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider, queryObjs: TQueryObj[]) => Promise<TResp[]>;
  // we are doing this much rpc calls per input object
  // this is used to calculate the input batch to send to the client
  // and to know if we can inject the batch provider or if we should use the regular provider
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  // errors
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter;
  logInfos: { msg: string; data?: Record<string, unknown> };
  // output
  formatOutput: (objs: TInputObj, results: TResp) => TRes;
}) {
  const retryConfig = getRpcRetryConfig({ logInfos: options.logInfos, maxTotalRetryMs: options.streamConfig.maxTotalRetryMs });

  // get the rpc provider maximum batch size
  const methodLimitations = options.rpcConfig.maxBatchProviderSize;

  // find out the max number of objects to process in a batch
  let canUseBatchProvider = true;
  let maxInputObjsPerBatch = options.streamConfig.maxInputTake;
  for (const [method, count] of Object.entries(options.rpcCallsPerInputObj)) {
    const maxCount = methodLimitations[method as RpcCallMethod];
    if (maxCount === null) {
      canUseBatchProvider = false;
      break;
    }
    maxInputObjsPerBatch = Math.min(maxInputObjsPerBatch, Math.floor(maxCount / count));
  }

  if (!canUseBatchProvider) {
    maxInputObjsPerBatch = options.streamConfig.maxInputTake;
  }

  return Rx.pipe(
    // take a batch of items
    Rx.bufferTime<TInputObj>(options.streamConfig.maxInputWaitMs, undefined, maxInputObjsPerBatch),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs: TInputObj[]) => {
      const contractCalls = objs.map((obj) => options.getQuery(obj));

      try {
        // retry the call if it fails
        const responses = await backOff(() => {
          // acquire a lock for this chain so in case we do use batch provider, we are guaranteed
          // the we are only batching the current request size and not more
          logger.trace({
            msg: "Acquiring provider lock for chain. " + options.logInfos.msg,
            data: { chain: options.rpcConfig.chain, ...options.logInfos.data },
          });
          return chainLock.acquire(options.rpcConfig.chain, async () => {
            logger.trace({
              msg: "Lock acquired for chain. " + options.logInfos.msg,
              data: { chain: options.rpcConfig.chain, ...options.logInfos.data },
            });
            const provider = canUseBatchProvider ? options.rpcConfig.batchProvider : options.rpcConfig.linearProvider;
            return options.processBatch(provider, contractCalls);
          });
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

    // flatten
    Rx.mergeAll(),
  );
}
