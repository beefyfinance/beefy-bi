import { zipWith } from "lodash";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "db-batch" });

export function dbBatchCall$<TObj, TCtx extends ImportCtx<TObj>, TRes, TQueryData, TQueryRes>(options: {
  ctx: TCtx;
  getData: (obj: TObj) => TQueryData;
  processBatch: (objAndData: { obj: TObj; data: TQueryData }[]) => Promise<TQueryRes[]>;
  formatOutput: (obj: TObj, res: TQueryRes) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),
    Rx.filter((items) => items.length > 0),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
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
          options.ctx.emitErrors(obj);
        }
        return [];
      }
    }, options.ctx.streamConfig.workConcurrency),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}
