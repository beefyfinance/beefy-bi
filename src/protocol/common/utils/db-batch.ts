import * as Rx from "rxjs";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "db-batch" });

export function dbBatchCall$<TObj, TCtx extends ImportCtx<TObj>, TRes, TQueryData, TQueryRes>(options: {
  ctx: TCtx;
  getData: (obj: TObj) => TQueryData;
  processBatch: (objAndData: { obj: TObj; data: TQueryData }[]) => Promise<Map<TQueryData, TQueryRes>>;
  logInfos: LogInfos;
  formatOutput: (obj: TObj, res: TQueryRes) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(options.ctx.streamConfig.dbMaxInputWaitMs, undefined, options.ctx.streamConfig.dbMaxInputTake),
    Rx.filter((items) => items.length > 0),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      try {
        const objAndData = objs.map((obj) => ({ obj, data: options.getData(obj) }));

        const resultMap = await options.processBatch(objAndData);
        return objAndData.map(({ obj, data }) => {
          if (!resultMap.has(data)) {
            logger.error(mergeLogsInfos({ msg: "result not found", data: { obj, data, resultMap } }, options.logInfos));
            throw new ProgrammerError(mergeLogsInfos({ msg: "result not found", data: { obj, data, resultMap } }, options.logInfos));
          }
          const res = resultMap.get(data) as TQueryRes;
          return options.formatOutput(obj, res);
        });
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
