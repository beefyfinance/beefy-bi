import { get } from "lodash";
import * as Rx from "rxjs";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { SupportedRangeTypes, rangeValueMax } from "../../../utils/range";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { fetchImportState$, updateImportState$ } from "../loader/import-state";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../types/import-query";
import { RunnerConfig, createChainRunner } from "./rpc-chain-runner";

const logger = rootLogger.child({ module: "common", component: "pipeline-error-handler" });

export function createImportStateUpdaterRunner<TInput, TRange extends SupportedRangeTypes>(options: {
  logInfos: LogInfos;
  cacheKey: string;
  runnerConfig: RunnerConfig<TInput>;
  getImportStateKey: (input: TInput) => string;
  pipeline$: (
    ctx: ImportCtx,
    emitError: ErrorEmitter<ImportRangeQuery<TInput, TRange>>,
    getLastImported: () => TRange,
  ) => Rx.OperatorFunction<TInput, ImportRangeResult<TInput, TRange>>;
}) {
  const lastImportCache: Record<string, TRange> = {};
  const createPipeline = (ctx: ImportCtx) => {
    const {
      observable: errorObs$,
      complete: completeErrorObs,
      next: emitError,
    } = createObservableWithNext<{ item: ImportRangeQuery<TInput, TRange>; report: ErrorReport }>();

    return Rx.pipe(
      // set ts typings
      Rx.tap((item: TInput) => logger.trace(mergeLogsInfos({ msg: "Handling input item", data: { item } }, options.logInfos))),

      // run the import
      options.pipeline$(
        ctx,
        (item, report) => emitError({ item, report }),
        () => lastImportCache[options.cacheKey],
      ),

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
                // errors should have been logged already
                logger.debug(
                  mergeLogsInfos(mergeLogsInfos({ msg: "Error handled during processing", data: { item } }, report.infos), options.logInfos),
                );
                logger.debug(report.error);
                return { ...item, success: false };
              }),
            ),
          ),
        ),

        Rx.pipe(
          // update the last processed cache
          Rx.tap((item) => {
            logger.trace(mergeLogsInfos({ msg: "updating recent import cache", data: { chain: ctx.chain, range: item.range } }, options.logInfos));
            if (item.success) {
              const cachedValue = lastImportCache[options.cacheKey];
              if (cachedValue === undefined) {
                lastImportCache[options.cacheKey] = item.range.to;
              } else {
                lastImportCache[options.cacheKey] = rangeValueMax([cachedValue, item.range.to]) as TRange;
              }
            }
          }),
          Rx.tap((item) => logger.trace(mergeLogsInfos({ msg: "processed", data: { range: item.range } }, options.logInfos))),
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
        ),

        Rx.tap({
          complete: () => logger.info(mergeLogsInfos({ msg: "Import end" }, options.logInfos)),
        }),
      ),
    );
  };

  return createChainRunner(options.runnerConfig, createPipeline);
}
