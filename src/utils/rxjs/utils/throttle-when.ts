import * as Rx from "rxjs";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../logger";
import { ProgrammerError } from "../../programmer-error";
import { createObservableWithNext } from "./create-observable-with-next";

const logger = rootLogger.child({ module: "rpc-utils", component: "throttle-when" });

export function throttleWhen<TObj>(options: {
  checkIntervalMs: number; // then every X ms
  checkIntervalJitterMs: number; // add a bit of jitter to the check interval
  sendBurstsOf: number; // how many items to send
  shouldSend: () => boolean; // should we send now?
  logInfos: LogInfos;
}): Rx.OperatorFunction<TObj, TObj> {
  const obss: (ReturnType<typeof createObservableWithNext> | null)[] = [];
  let firstIdx = 0;

  function poll() {
    const shouldSend = options.shouldSend();
    if (!shouldSend) {
      logger.debug(mergeLogsInfos({ msg: "not sending" }, options.logInfos));
      return;
    }

    if (firstIdx < obss.length) {
      logger.debug(mergeLogsInfos({ msg: "sending a burst", data: { sendBurstsOf: options.sendBurstsOf } }, options.logInfos));
    }

    for (let n = 0; n < options.sendBurstsOf; n++) {
      const idx = n + firstIdx;
      if (idx >= obss.length) {
        break;
      }
      const obs = obss[idx];
      if (obs === null) {
        throw new Error("should not happen");
      }
      logger.debug(mergeLogsInfos({ msg: "sending item", data: { idx, of: obss.length, sendBurstsOf: options.sendBurstsOf } }, options.logInfos));
      obs.next(1);
      obs.complete();
      obss[idx] = null;
    }
    firstIdx += options.sendBurstsOf;
  }
  let pollHandle: null | NodeJS.Timer = null;

  return Rx.pipe(
    Rx.delayWhen((_, index) => {
      const obsIndex = index % options.sendBurstsOf;
      if (!obss[obsIndex]) {
        obss[obsIndex] = createObservableWithNext();
      }
      const obs = obss[obsIndex];
      if (obs === null) {
        throw new Error("should not happen");
      }
      return obs.observable;
    }),

    Rx.tap({
      subscribe: () => {
        if (pollHandle !== null) {
          return;
        }
        logger.trace(
          mergeLogsInfos(
            { msg: "Starting poller", data: { checkIntervalMs: options.checkIntervalMs, checkIntervalJitterMs: options.checkIntervalJitterMs } },
            options.logInfos,
          ),
        );
        pollHandle = setInterval(poll, options.checkIntervalMs + Math.random() * options.checkIntervalJitterMs);
      },
    }),
  );
}
