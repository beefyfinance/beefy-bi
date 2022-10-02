import * as Rx from "rxjs";
import { backOff, IBackOffOptions } from "exponential-backoff";
import { flatten, zipWith } from "lodash";
import { ProgrammerError, shouldRetryProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { rootLogger } from "../../../utils/logger";
import { DbProduct } from "../loader/product";
import { shouldRetryRpcError } from "../../../lib/rpc/archive-node-needed";

const logger = rootLogger.child({ module: "utils", component: "batch-query-group" });

export interface BatchIntakeConfig {
  // how many items to take from the input stream before making groups
  take: number;
  // how long to wait before making groups
  maxWaitMs: number;
  // how many concurrent groups to process at the same time
  concurrency: number;
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
  intakeConfig: BatchIntakeConfig;
  getQueryForBatch: (obj: TInputObj[]) => TQueryObj;
  processBatchKey?: (obj: TInputObj) => string | number;
  processBatch: (queryObjs: TQueryObj[]) => Promise<TResp[]>;
  // errors
  emitErrors: ErrorEmitter;
  logInfos: { msg: string; data?: Record<string, unknown> };
  // output
  formatOutput: (objs: TInputObj, results: TResp) => TRes;
}): Rx.OperatorFunction<TInputObj, TRes> {
  const rpcRetryConfig: Partial<IBackOffOptions> = {
    // delays: 0.1s, 0.5s, 2.5s, 12.5s, 1m2.5s, 5m, 5m, 5m, 5m, 5m
    delayFirstAttempt: false,
    startingDelay: 100,
    timeMultiple: 5,
    maxDelay: 5 * 60 * 1000,
    numOfAttempts: 10,

    jitter: "full",
    retry: (error, attemptNumber) => {
      const shouldRetry = shouldRetryRpcError(error) && shouldRetryProgrammerError(error);
      if (shouldRetry) {
        const logMsg = { msg: `RPC Error caught, will retry: ${options.logInfos.msg}`, data: { ...options.logInfos.data, error } };
        if (attemptNumber < 3) logger.trace(logMsg);
        else if (attemptNumber < 5) logger.debug(logMsg);
        else if (attemptNumber < 9) logger.info(logMsg);
        else if (attemptNumber < 10) logger.warn(logMsg);
        else {
          logger.error(logMsg);
          logger.error(error);
        }
      } else {
        logger.debug({ msg: `Unretryable error caught, will not retry, ${options.logInfos.msg}`, data: { ...options.logInfos.data, error } });
        logger.error(error);
      }
      return shouldRetry;
    },
  };

  return (objs$) =>
    objs$.pipe(
      // take a batch of items
      Rx.bufferTime(options.intakeConfig.maxWaitMs, undefined, options.intakeConfig.take),

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
                results = await backOff(() => options.processBatch(queries.map((q) => q.query)), rpcRetryConfig);
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
            }, options.intakeConfig.concurrency),
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
