import { get, sortBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { DbClient } from "../../../utils/db";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { Range, rangeExcludeMany, rangeValueMax, SupportedRangeTypes } from "../../../utils/range";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { addMissingImportState$, DbImportState, fetchImportState$, updateImportState$ } from "../loader/import-state";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../types/import-query";
import { BatchStreamConfig } from "./batch-rpc-calls";
import { createRpcConfig } from "./rpc-config";

const logger = rootLogger.child({ module: "common", component: "historical-import" });

export const defaultHistoricalStreamConfig: BatchStreamConfig = {
  // since we are doing many historical queries at once, we cannot afford to do many at once
  workConcurrency: 1,
  // But we can afford to wait a bit longer before processing the next batch to be more efficient
  maxInputWaitMs: 30 * 1000,
  maxInputTake: 500,
  dbMaxInputTake: BATCH_DB_INSERT_SIZE,
  dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
  // and we can affort longer retries
  maxTotalRetryMs: 30_000,
};
export const defaultMoonbeamHistoricalStreamConfig: BatchStreamConfig = {
  // since moonbeam is so unreliable but we already have a lot of data, we can afford to do 1 at a time
  workConcurrency: 1,
  maxInputWaitMs: 1000,
  maxInputTake: 1,
  dbMaxInputTake: 1,
  dbMaxInputWaitMs: 1,
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

  dbMaxInputTake: BATCH_DB_INSERT_SIZE,
  dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
  // and we cannot afford too long of a retry per product
  maxTotalRetryMs: 10_000,
};

export function createHistoricalImportPipeline<TInput, TRange extends SupportedRangeTypes, TImport extends DbImportState>(options: {
  client: DbClient;
  chain: Chain;
  logInfos: LogInfos;
  getImportStateKey: (input: TInput) => string;
  isLiveItem: (input: TInput) => boolean;
  createDefaultImportState$: (ctx: ImportCtx, emitError: ErrorEmitter<TInput>) => Rx.OperatorFunction<TInput, TImport["importData"]>;
  generateQueries$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<{ target: TInput; importState: TImport }>,
  ) => Rx.OperatorFunction<{ target: TInput; importState: TImport }, ImportRangeQuery<TInput, TRange> & { importState: TImport }>;
  processImportQuery$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<ImportRangeQuery<TInput, TRange>>,
  ) => Rx.OperatorFunction<ImportRangeQuery<TInput, TRange> & { importState: TImport }, ImportRangeResult<TInput, TRange>>;
}) {
  const streamConfig = options.chain === "moonbeam" ? defaultMoonbeamHistoricalStreamConfig : defaultHistoricalStreamConfig;
  const { observable: errorObs$, complete: completeErrorObs, next: emitError } = createObservableWithNext<ImportRangeQuery<TInput, TRange>>();

  const ctx: ImportCtx = {
    chain: options.chain,
    client: options.client,
    rpcConfig: createRpcConfig(options.chain),
    streamConfig,
  };

  return Rx.pipe(
    addMissingImportState$({
      client: options.client,

      streamConfig,
      getImportStateKey: options.getImportStateKey,
      createDefaultImportState$: options.createDefaultImportState$(ctx, (obj) => {
        logger.error(mergeLogsInfos({ msg: "Error while creating default import state", data: { obj } }, options.logInfos));
      }),
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
    options.generateQueries$(ctx, (item) => {
      logger.error(mergeLogsInfos({ msg: "Error while generating queries", data: { item } }, options.logInfos));
    }),

    Rx.tap((item) =>
      logger.info(
        mergeLogsInfos(
          { msg: "processing query", data: { chain: ctx.chain, range: item.range, importStateKey: item.importState.importKey } },
          options.logInfos,
        ),
      ),
    ),

    // run the import
    options.processImportQuery$(ctx, emitError),

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
        ctx,
        emitError: (item) => {
          logger.error(mergeLogsInfos({ msg: "error while updating import state", data: item }, options.logInfos));
        },
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
  client: DbClient;
  chain: Chain;
  logInfos: LogInfos;
  cacheKey: string;
  getImportStateKey: (input: TInput) => string;
  isLiveItem: (input: TInput) => boolean;
  generateQueries$: <TGQIn extends { target: TInput }, TGQRes extends ImportRangeQuery<TInput, TRange>[]>(options: {
    ctx: ImportCtx;
    emitError: ErrorEmitter<TGQIn>;
    lastImported: TRange | null;
    formatOutput: (item: TGQIn, latest: TRange, queries: Range<TRange>[]) => TGQRes;
  }) => Rx.OperatorFunction<TGQIn, TGQRes>;
  processImportQuery$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<ImportRangeQuery<TInput, TRange>>,
  ) => Rx.OperatorFunction<ImportRangeQuery<TInput, TRange>, ImportRangeResult<TInput, TRange>>;
}) {
  const streamConfig = defaultRecentStreamConfig;

  const ctx: ImportCtx = {
    chain: options.chain,
    client: options.client,
    rpcConfig: createRpcConfig(options.chain),
    streamConfig,
  };

  return Rx.pipe(
    // only process live items
    Rx.filter((item: TInput) => options.isLiveItem(item)),

    // make is a query
    Rx.map((target) => ({ target })),

    Rx.pipe(
      // find the import state if it exists so we can resume from there
      fetchImportState$({
        client: ctx.client,
        streamConfig: ctx.streamConfig,
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        formatOutput: (item, importState) => ({ ...item, importState }),
      }),

      // create the import queries
      options.generateQueries$({
        ctx,
        emitError: (obj) => {
          logger.error(mergeLogsInfos({ msg: "Error while generating import query", data: { obj } }, options.logInfos));
        },
        lastImported: (recentImportCache[options.cacheKey] ?? null) as TRange | null,
        formatOutput: (item, latest, ranges) => ranges.map((range) => ({ ...item, latest, range })),
      }),
      Rx.concatAll(),
      Rx.filter((item) => {
        if (item.importState === null) {
          return true;
        }
        let range = item.range;
        let ranges = rangeExcludeMany(range, item.importState.importData.ranges.coveredRanges as Range<TRange>[]);
        // if we already covered the range, skip it
        if (ranges.length === 0) {
          logger.debug({ msg: "skipping import query, already imported", data: { chain: ctx.chain, range, importState: item.importState } });
          return false;
        } else {
          return true;
        }
      }),

      Rx.tap((item) => logger.info(mergeLogsInfos({ msg: "processing query", data: { chain: ctx.chain, range: item.range } }, options.logInfos))),
    ),

    // run the import
    options.processImportQuery$(ctx, (obj) => {
      // since we are doing
      logger.error(mergeLogsInfos({ msg: "Error while processing import query", data: { obj } }, options.logInfos));
    }),

    Rx.pipe(
      // update the last processed cache
      Rx.tap((item) => {
        logger.trace(mergeLogsInfos({ msg: "updating recent import cache", data: { chain: ctx.chain, range: item.range } }, options.logInfos));
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
    ),
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
        ctx,
        emitError: (item) => {
          logger.error(mergeLogsInfos({ msg: "error while updating import state", data: item }, options.logInfos));
        },
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        getRange: (item) => item.range,
        isSuccess: (item) => item.success,
        formatOutput: (item) => item,
      }),
      Rx.tap({
        complete: () => logger.info(mergeLogsInfos({ msg: "Recent data import end" }, options.logInfos)),
      }),
    ),
  );
}
