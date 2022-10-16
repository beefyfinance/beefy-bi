import { keyBy, uniqBy, zipWith } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { SupportedRangeTypes } from "../../../utils/range";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { BatchStreamConfig } from "./batch-rpc-calls";

const logger = rootLogger.child({ module: "common", component: "db-batch" });

export function dbBatchCall$<
  TTarget,
  TRange extends SupportedRangeTypes,
  TObj extends ImportQuery<TTarget, TRange>,
  TRes extends ImportQuery<TTarget, TRange>,
  TQueryRes,
  TQueryData,
>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter<TTarget, TRange>;
  getData: (obj: TObj) => TQueryData;
  processBatch: (objAndData: { obj: TObj; data: TQueryData }[]) => Promise<TQueryRes[]>;
  formatOutput: (obj: TObj, res: TQueryRes) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }
      try {
        const objAndData = objs.map((obj) => ({ obj, data: options.getData(obj) }));

        const results = await options.processBatch(objAndData);
        if (results.length !== objs.length) {
          throw new Error(`Expected ${objs.length} results, got ${results.length}`);
        }
        return zipWith(objs, results, (obj, res) => options.formatOutput(obj, res));
      } catch (err) {
        logger.error({ msg: "Error inserting batch", data: { objsLen: objs.length, err } });
        logger.error(err);
        for (const obj of objs) {
          options.emitErrors(obj);
        }
        return [];
      }
    }, options.streamConfig.workConcurrency),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}