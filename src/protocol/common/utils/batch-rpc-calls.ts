import * as Rx from "rxjs";
import { backOff } from "exponential-backoff";
import { flatten, zipWith } from "lodash";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { rootLogger } from "../../../utils/logger";
import { DbProduct } from "../loader/product";
import { getRpcRetryConfig } from "./rpc-retry-config";
import { RpcConfig } from "../../../types/rpc-config";

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

export function batchRpcCalls$<
  TProduct extends DbProduct,
  TInputObj extends ProductImportQuery<TProduct>,
  TQueryObj,
  TResp,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  rpcConfig: RpcConfig;
  getQuery: (obj: TInputObj) => TQueryObj;
  processBatch: (queryObjs: TQueryObj[]) => Promise<TResp[]>;
  // errors
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter;
  logInfos: { msg: string; data?: Record<string, unknown> };
  // output
  formatOutput: (objs: TInputObj, results: TResp) => TRes;
}) {
  const retryConfig = getRpcRetryConfig({ logInfos: options.logInfos, maxTotalRetryMs: options.streamConfig.maxTotalRetryMs });

  return Rx.pipe(
    // take a batch of items
    Rx.bufferTime<TInputObj>(
      options.streamConfig.maxInputWaitMs,
      undefined,
      Math.min(options.streamConfig.maxInputTake, options.rpcConfig.maxBatchProviderSize),
    ),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs: TInputObj[]) => {
      const contractCalls = objs.map((obj) => options.getQuery(obj));

      try {
        const responses = await backOff(() => options.processBatch(contractCalls), retryConfig);
        if (responses.length !== objs.length) {
          throw new ProgrammerError({
            msg: "Query and result length mismatch",
            data: { contractCallsCount: contractCalls.length, responsesLength: responses.length },
          });
        }
        return zipWith(objs, responses, options.formatOutput);
      } catch (err) {
        // here, none of the retrying worked, so we emit all the objects as in error
        logger.error({ msg: "Error doing batch rpc work: " + options.logInfos.msg, err });
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
