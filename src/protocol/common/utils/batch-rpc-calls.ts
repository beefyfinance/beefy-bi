import * as Rx from "rxjs";
import { backOff } from "exponential-backoff";
import { flatten, zipWith } from "lodash";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { rootLogger } from "../../../utils/logger";
import { DbProduct } from "../loader/product";
import { getRpcRetryConfig } from "./rpc-retry-config";

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

/**
 * Tool to make batch rpc calls
 * We work on ProductImportQuery<> items to be able to treat errors properly
 *
 * What it does:
 *   - take some amount of input objects
 *   - make groups of calls from the batch
 *   - make a call to processBatch for each group
 *   - retry errors
 *   - send true errors to the error pipeline
 *
 */
export function batchRpcCalls$<
  TProduct extends DbProduct,
  TInputObj extends ProductImportQuery<TProduct>,
  TQueryObj,
  TResp,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  streamConfig: BatchStreamConfig;
  getQueryForBatch: (obj: TInputObj[]) => TQueryObj;
  processBatchKey?: (obj: TInputObj) => string | number;
  processBatch: (queryObjs: TQueryObj[]) => Promise<TResp[]>;
  // errors
  emitErrors: ErrorEmitter;
  logInfos: { msg: string; data?: Record<string, unknown> };
  // output
  formatOutput: (objs: TInputObj, results: TResp) => TRes;
}): Rx.OperatorFunction<TInputObj, TRes> {
  const retryConfig = getRpcRetryConfig({ logInfos: options.logInfos, maxTotalRetryMs: options.streamConfig.maxTotalRetryMs });

  return Rx.pipe(
    // take a batch of items
    Rx.bufferTime(options.streamConfig.maxInputWaitMs, undefined, options.streamConfig.maxInputTake),

    Rx.concatMap((objs) => {
      const pipeline$ = Rx.from(objs)
        // extract query objects by key
        .pipe(
          Rx.groupBy(options.processBatchKey ? options.processBatchKey : () => ""),
          Rx.mergeMap((group$) => group$.pipe(Rx.toArray())),
          Rx.map((objs) => ({ query: options.getQueryForBatch(objs), objs })),
          Rx.toArray(),
        )
        // make a batch query for eatch key
        .pipe(
          Rx.mergeMap(async (queries) => {
            let results: TResp[];
            try {
              results = await backOff(() => options.processBatch(queries.map((q) => q.query)), retryConfig);
            } catch (err) {
              // here, none of the retrying worked, so we emit all the objects as in error
              logger.error({ msg: "Error in batch query", err });
              logger.error(err);
              const errorObjs = flatten(queries.map((q) => q.objs));
              for (const errorObj of errorObjs) {
                options.emitErrors(errorObj);
              }
              return Rx.EMPTY;
            }

            // assuming the process function returns the results in the same order as the input
            if (results.length !== queries.length) {
              throw new ProgrammerError({ msg: "Query and result length mismatch", queries, results });
            }
            return zipWith(queries, results, (q, r) => ({ ...q, result: r }));
          }, options.streamConfig.workConcurrency),
          Rx.mergeAll(),
        )
        // re-emit all input objects with the corresponding result
        .pipe(
          Rx.map((resp) => resp.objs.map((obj) => options.formatOutput(obj, resp.result))),
          Rx.mergeAll(),
          Rx.toArray(),
        );
      return pipeline$;
    }),

    // flatten the results
    Rx.concatMap((objBatch) => Rx.from(objBatch)),
  );
}
