import { get, sortBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { DISABLE_RECENT_IMPORT_SKIP_ALREADY_IMPORTED } from "../../../utils/config";
import { DbClient } from "../../../utils/db";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { Range, rangeExcludeMany, rangeValueMax, SupportedRangeTypes } from "../../../utils/range";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { addMissingImportState$, DbImportState, fetchImportState$, updateImportState$ } from "../loader/import-state";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../types/import-query";
import { multiplexByRcp } from "./multiplex-by-rpc";

const logger = rootLogger.child({ module: "common", component: "historical-import" });

export function createHistoricalImportPipeline<TInput, TRange extends SupportedRangeTypes, TImport extends DbImportState>(options: {
  client: DbClient;
  chain: Chain;
  rpcCount: number;
  logInfos: LogInfos;
  forceRpcUrl: string | null;
  forceGetLogsBlockSpan: number | null;
  getImportStateKey: (input: TInput) => string;
  isLiveItem: (input: TInput) => boolean;
  createDefaultImportState$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<TInput>,
  ) => Rx.OperatorFunction<TInput, { obj: TInput; importData: TImport["importData"] }>;
  generateQueries$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<{ target: TInput; importState: TImport }>,
  ) => Rx.OperatorFunction<{ target: TInput; importState: TImport }, ImportRangeQuery<TInput, TRange> & { importState: TImport }>;
  processImportQuery$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<ImportRangeQuery<TInput, TRange>>,
  ) => Rx.OperatorFunction<ImportRangeQuery<TInput, TRange> & { importState: TImport }, ImportRangeResult<TInput, TRange>>;
}) {
  const createPipeline = (ctx: ImportCtx) => {
    const {
      observable: errorObs$,
      complete: completeErrorObs,
      next: emitError,
    } = createObservableWithNext<{ item: ImportRangeQuery<TInput, TRange>; report: ErrorReport }>();

    return Rx.pipe(
      addMissingImportState$({
        client: options.client,
        streamConfig: ctx.streamConfig,
        getImportStateKey: options.getImportStateKey,
        createDefaultImportState$: options.createDefaultImportState$(ctx, (obj, report) => {
          logger.error(
            mergeLogsInfos(
              mergeLogsInfos({ msg: "Error while creating default import state", data: { obj, error: report.error } }, report.infos),
              options.logInfos,
            ),
          );
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
      options.generateQueries$(ctx, (item, report) => {
        logger.error(
          mergeLogsInfos(
            mergeLogsInfos({ msg: "Error while generating queries", data: { ...item, error: report.error } }, report.infos),
            options.logInfos,
          ),
        );
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
      options.processImportQuery$(ctx, (item, report) => emitError({ item, report })),

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
          Rx.mergeWith(
            errorObs$.pipe(
              Rx.map(({ item, report }) => {
                logger.error(
                  mergeLogsInfos(mergeLogsInfos({ msg: "Error processing historical query", data: { item } }, report.infos), options.logInfos),
                );
                logger.error(report.error);
                return { ...item, success: false };
              }),
            ),
          ),
        ),

        // update the import state
        updateImportState$({
          ctx,
          emitError: (item, report) => {
            logger.error(
              mergeLogsInfos(mergeLogsInfos({ msg: "Error while updating import state", data: { item } }, report.infos), options.logInfos),
            );
            logger.error(report.error);
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
  };

  return Rx.pipe(
    // only process live items
    Rx.filter((item: TInput) => options.isLiveItem(item)),
    multiplexByRcp({
      chain: options.chain,
      client: options.client,
      mode: "historical",
      rpcCount: options.rpcCount,
      forceRpcUrl: options.forceRpcUrl,
      forceGetLogsBlockSpan: options.forceGetLogsBlockSpan,
      createPipeline,
    }),
  );
}

const recentImportCache: Record<string, SupportedRangeTypes> = {};
export function createRecentImportPipeline<TInput, TRange extends SupportedRangeTypes>(options: {
  client: DbClient;
  chain: Chain;
  rpcCount: number;
  forceRpcUrl: string | null;
  forceGetLogsBlockSpan: number | null;
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
  const createPipeline = (ctx: ImportCtx) =>
    Rx.pipe(
      // set ts typings
      Rx.map((item: TInput) => ({ target: item })),

      // find the import state if it exists so we can resume from there
      fetchImportState$({
        client: ctx.client,
        streamConfig: ctx.streamConfig,
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        formatOutput: (item, importState) => ({ ...item, importState }),
      }),

      Rx.pipe(
        // create the import queries
        options.generateQueries$({
          ctx,
          emitError: (obj, report) => {
            logger.error(
              mergeLogsInfos(mergeLogsInfos({ msg: "Error while generating import query", data: { obj } }, report.infos), options.logInfos),
            );
            logger.error(report.error);
          },
          lastImported: (recentImportCache[options.cacheKey] ?? null) as TRange | null,
          formatOutput: (item, latest, ranges) => ranges.map((range) => ({ ...item, latest, range })),
        }),
        Rx.concatAll(),
        Rx.filter((item) => {
          // sometimes we want to manually re-import some data
          if (DISABLE_RECENT_IMPORT_SKIP_ALREADY_IMPORTED) {
            return true;
          }
          // is the import state has not been created, we still import the data
          if (item.importState === null) {
            return true;
          }
          let range = item.range;
          let ranges = rangeExcludeMany(range, item.importState.importData.ranges.coveredRanges as Range<TRange>[]);
          // if we already covered the range, skip it
          if (ranges.length === 0) {
            logger.debug({
              msg: "skipping import query, already imported, set DISABLE_RECENT_IMPORT_SKIP_ALREADY_IMPORTED=true to import anyway",
              data: { chain: ctx.chain, range, importState: item.importState },
            });
            return false;
          } else {
            return true;
          }
        }),

        Rx.tap((item) => logger.info(mergeLogsInfos({ msg: "processing query", data: { chain: ctx.chain, range: item.range } }, options.logInfos))),
      ),

      // run the import
      options.processImportQuery$(ctx, (obj, report) => {
        // since we are doing
        logger.error(mergeLogsInfos({ msg: "Error while processing import query", data: { obj, report } }, options.logInfos));
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
          emitError: (item, report) => {
            logger.error(
              mergeLogsInfos(mergeLogsInfos({ msg: "Error while updating import state", data: { item } }, report.infos), options.logInfos),
            );
            logger.error(report.error);
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

  return Rx.pipe(
    // only process live items
    Rx.filter((item: TInput) => options.isLiveItem(item)),
    multiplexByRcp({
      chain: options.chain,
      client: options.client,
      mode: "recent",
      rpcCount: options.rpcCount,
      forceRpcUrl: options.forceRpcUrl,
      forceGetLogsBlockSpan: options.forceGetLogsBlockSpan,
      createPipeline,
    }),
  );
}
