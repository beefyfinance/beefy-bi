import { get, sortBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { Range, rangeValueMax, SupportedRangeTypes } from "../../../utils/range";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { addMissingImportState$, DbImportState, fetchImportState$, updateImportState$ } from "../loader/import-state";
import { ImportCtx } from "../types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../types/import-query";
import { BatchStreamConfig } from "./batch-rpc-calls";
import { memoryBackpressure$ } from "./memory-backpressure";
import { createRpcConfig } from "./rpc-config";

const logger = rootLogger.child({ module: "common", component: "historical-import" });

export const defaultHistoricalStreamConfig: BatchStreamConfig = {
  // since we are doing many historical queries at once, we cannot afford to do many at once
  workConcurrency: 1,
  // But we can afford to wait a bit longer before processing the next batch to be more efficient
  maxInputWaitMs: 30 * 1000,
  maxInputTake: 500,
  // and we can affort longer retries
  maxTotalRetryMs: 30_000,
};
export const defaultRecentStreamConfig: BatchStreamConfig = {
  // since we are doing live data on a small amount of queries (one per vault)
  // we can afford some amount of concurrency
  workConcurrency: 10,
  // But we can not afford to wait before processing the next batch
  maxInputWaitMs: 5_000,
  maxInputTake: 500,
  // and we cannot afford too long of a retry per product
  maxTotalRetryMs: 10_000,
};

export function createHistoricalImportPipeline<TInput, TRange extends SupportedRangeTypes, TImport extends DbImportState>(options: {
  client: PoolClient;
  chain: Chain;
  logInfos: LogInfos;
  getImportStateKey: (input: TInput) => string;
  isLiveItem: (input: TInput) => boolean;
  createDefaultImportState$: (ctx: ImportCtx<ImportRangeQuery<TInput, TRange>>) => Rx.OperatorFunction<TInput, TImport["importData"]>;
  generateQueries$: (ctx: ImportCtx<ImportRangeQuery<TInput, TRange>>) => Rx.OperatorFunction<
    { target: TInput; importState: TImport },
    {
      range: Range<TRange>;
      latest: TRange;
    }[]
  >;
  processImportQuery$: (
    ctx: ImportCtx<ImportRangeQuery<TInput, TRange>>,
  ) => Rx.OperatorFunction<ImportRangeQuery<TInput, TRange> & { importState: TImport }, ImportRangeResult<TInput, TRange>>;
}) {
  const streamConfig = defaultHistoricalStreamConfig;
  const { observable: errorObs$, complete: completeErrorObs, next: emitErrors } = createObservableWithNext<ImportRangeQuery<TInput, TRange>>();

  const ctx: ImportCtx<ImportRangeQuery<TInput, TRange>> = {
    client: options.client,
    emitErrors,
    rpcConfig: createRpcConfig(options.chain),
    streamConfig,
  };

  const createDefaultImportState$ = options.createDefaultImportState$(ctx);
  const processImportQuery$ = options.processImportQuery$(ctx);
  const generateQueries$ = options.generateQueries$(ctx);

  return Rx.pipe(
    addMissingImportState$({
      client: options.client,
      streamConfig,
      getImportStateKey: options.getImportStateKey,
      createDefaultImportState$,
      formatOutput: (target, importState) => ({ target, importState }),
    }),

    // first process the import states we imported the least
    Rx.pipe(
      Rx.toArray(),
      Rx.map((items) =>
        sortBy(
          items,
          (item) =>
            item.importState.importData.ranges.lastImportDate.getTime() + (options.isLiveItem(item.target) ? 0 : samplingPeriodMs["1day"] * 100),
        ),
      ),
      Rx.concatAll(),
    ),

    // create the queries
    Rx.concatMap((item) =>
      Rx.of(item).pipe(
        generateQueries$,
        Rx.concatMap((queries) => queries.map((query) => ({ ...item, ...query }))),
      ),
    ),

    // some backpressure mechanism
    Rx.pipe(
      memoryBackpressure$({
        logInfos: options.logInfos,
        sendBurstsOf: streamConfig.maxInputTake,
      }),
      Rx.tap((item) =>
        logger.info(
          mergeLogsInfos({ msg: "processing query", data: { range: item.range, importStateKey: item.importState.importKey } }, options.logInfos),
        ),
      ),
    ),

    // run the import
    processImportQuery$,

    Rx.pipe(
      // handle the results
      Rx.pipe(
        Rx.map((item) => ({
          ...item,
          // assign success to the import state if not already set
          success: get(item, "success", true) as boolean,
        })),
        // make sure we close the errors observable when we are done
        Rx.finalize(() => setTimeout(completeErrorObs)),
        // merge the errors back in, all items here should have been successfully treated
        Rx.mergeWith(errorObs$.pipe(Rx.map((item) => ({ ...item, success: false })))),
      ),

      // update the import state
      updateImportState$({
        client: options.client,
        streamConfig,
        getRange: (item) => item.range,
        isSuccess: (item) => item.success,
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        formatOutput: (item) => item,
      }),

      Rx.tap({
        complete: () => logger.info(mergeLogsInfos({ msg: "Historical data import end" }, options.logInfos)),
      }),
    ),
  );
}

const recentImportCache: Record<string, SupportedRangeTypes> = {};
export function createRecentImportPipeline<TInput, TRange extends SupportedRangeTypes>(options: {
  client: PoolClient;
  chain: Chain;
  logInfos: LogInfos;
  cacheKey: string;
  getImportStateKey: (input: TInput) => string;
  isLiveItem: (input: TInput) => boolean;
  generateQueries$: (
    ctx: ImportCtx<ImportRangeQuery<TInput, TRange>>,
    lastImported: TRange | null,
  ) => Rx.OperatorFunction<
    { target: TInput },
    {
      range: Range<TRange>;
      latest: TRange;
    }[]
  >;
  processImportQuery$: (
    ctx: ImportCtx<ImportRangeQuery<TInput, TRange>>,
  ) => Rx.OperatorFunction<ImportRangeQuery<TInput, TRange>, ImportRangeResult<TInput, TRange>>;
}) {
  const streamConfig = defaultRecentStreamConfig;

  const ctx: ImportCtx<ImportRangeQuery<TInput, TRange>> = {
    client: options.client,
    emitErrors: () => {}, // ignore errors since we are doing live data
    rpcConfig: createRpcConfig(options.chain),
    streamConfig,
  };

  const processImportQuery$ = options.processImportQuery$(ctx);
  const generateQueries$ = options.generateQueries$(ctx, (recentImportCache[options.cacheKey] ?? null) as TRange | null);

  // maintain the latest processed range value for each chain so we don't reprocess the same data again and again

  return Rx.pipe(
    // only process live items
    Rx.filter((item: TInput) => options.isLiveItem(item)),
    // make is a query
    Rx.map((target) => ({ target })),

    // create the import queries
    Rx.concatMap((item) =>
      Rx.of(item).pipe(
        generateQueries$,
        Rx.concatMap((queries) => queries.map((query) => ({ ...item, ...query }))),
      ),
    ),

    // run the import
    processImportQuery$,

    // update the last processed cache
    Rx.tap((item) => {
      logger.trace(mergeLogsInfos({ msg: "updating recent import cache", data: { range: item.range } }, options.logInfos));
      if (item.success) {
        const cachedValue = recentImportCache[options.cacheKey];
        if (cachedValue === undefined) {
          recentImportCache[options.cacheKey] = item.range.to;
        } else {
          recentImportCache[options.cacheKey] = rangeValueMax([cachedValue, item.range.to]) as TRange;
        }
      }
    }),

    Rx.tap((item) => logger.trace(mergeLogsInfos({ msg: "processed recent import", data: { range: item.range } }, options.logInfos))),

    // update the import state for those who have one
    Rx.pipe(
      fetchImportState$({
        client: ctx.client,
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        streamConfig: ctx.streamConfig,
        formatOutput: (item, importState) => ({ ...item, importState }),
      }),
      excludeNullFields$("importState"),
      updateImportState$({
        client: ctx.client,
        streamConfig: ctx.streamConfig,
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        getRange: (item) => item.range,
        isSuccess: (item) => item.success,
        formatOutput: (item) => item,
      }),
    ),

    Rx.tap({
      complete: () => logger.info(mergeLogsInfos({ msg: "Recent data import end" }, options.logInfos)),
    }),
  );
}
