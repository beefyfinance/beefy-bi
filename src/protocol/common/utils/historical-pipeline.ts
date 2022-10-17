import { get, sortBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { samplingPeriodMs } from "../../../types/sampling";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { Range, SupportedRangeTypes } from "../../../utils/range";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { addMissingImportState$, DbImportState, updateImportState$ } from "../loader/import-state";
import { ImportQuery } from "../types/import-query";
import { BatchStreamConfig } from "./batch-rpc-calls";
import { memoryBackpressure$ } from "./memory-backpressure";

const logger = rootLogger.child({ module: "common", component: "historical-import" });

export function createHistoricalImportPipeline<TInput, TRange extends SupportedRangeTypes, TImport extends DbImportState>(options: {
  client: PoolClient;
  logInfos: LogInfos;
  getImportStateKey: (input: TInput) => string;
  createDefaultImportState$: Rx.OperatorFunction<TInput, TImport["importData"]>;
  isLiveItem: (input: TInput) => boolean;
  generateQueries$: Rx.OperatorFunction<
    { target: TInput; importState: TImport },
    {
      range: Range<TRange>;
      latest: TRange;
    }[]
  >;
  processImportQuery$: Rx.MonoTypeOperatorFunction<ImportQuery<TInput, TRange>>;
}) {
  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };
  const { observable: errorObs$, complete: completeErrorObs, next: emitErrors } = createObservableWithNext<ImportQuery<TInput, TRange>>();

  return Rx.pipe(
    addMissingImportState$({
      client: options.client,
      streamConfig,
      getImportStateKey: options.getImportStateKey,
      createDefaultImportState$: options.createDefaultImportState$,
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
        options.generateQueries$,
        Rx.concatMap((queries) => queries.map((query) => ({ ...item, ...query }))),
      ),
    ),

    // some backpressure mechanism
    Rx.pipe(
      memoryBackpressure$({
        logInfos: { msg: "import-price-data" },
        sendBurstsOf: streamConfig.maxInputTake,
      }),
      Rx.tap((item) => logger.info(mergeLogsInfos({ msg: "processing query", data: { range: item.range } }, options.logInfos))),
    ),

    // run the import
    options.processImportQuery$,

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
        getImportStateKey: (item) => options.getImportStateKey(item.target),
        formatOutput: (item) => item,
      }),
    ),
  );
}
