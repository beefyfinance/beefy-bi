import * as Rx from "rxjs";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "rxjs-utils", component: "log-observable-lifecycle" });

export function logObservableLifecycle<TObj>(logInfos: LogInfos): Rx.MonoTypeOperatorFunction<TObj> {
  return Rx.tap({
    subscribe: () => logger.info(mergeLogsInfos({ msg: "SUBSCRIBE" }, logInfos)),
    unsubscribe: () => logger.info(mergeLogsInfos({ msg: "UNSUBSCRIBE" }, logInfos)),
    finalize: () => logger.info(mergeLogsInfos({ msg: "FINALIZE" }, logInfos)),
    complete: () => logger.info(mergeLogsInfos({ msg: "COMPLETE" }, logInfos)),
    next: (obj) => logger.trace(mergeLogsInfos({ msg: "NEXT", data: { obj } }, logInfos)),
    error: (err) => {
      logger.error(mergeLogsInfos({ msg: "ERROR", data: { err } }, logInfos));
      logger.error(err);
    },
  });
}
