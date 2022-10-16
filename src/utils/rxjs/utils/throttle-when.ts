import * as Rx from "rxjs";
import { createObservableWithNext } from "./create-observable-with-next";

export function throttleWhen<TObj>(options: {
  checkIntervalMs: number; // then every X ms
  checkIntervalJitterMs: number; // add a bit of jitter to the check interval
  sendBurstsOf: number; // how many items to send
  shouldSend: () => boolean; // should we send now?
  logInfos: { msg: string; data?: object };
}): Rx.OperatorFunction<TObj, TObj> {
  const obss: (ReturnType<typeof createObservableWithNext> | null)[] = [];
  let firstIdx = 0;

  const poller = setInterval(() => {
    const shouldSend = options.shouldSend();
    if (!shouldSend) {
      return;
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
      obs.next(1);
      obs.complete();
      obss[idx] = null;
    }
    firstIdx += options.sendBurstsOf;
  }, options.checkIntervalMs + Math.random() * options.checkIntervalJitterMs);

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

    Rx.finalize(() => {
      clearInterval(poller);
      const openObss = obss.filter((obs) => obs !== null);
      if (openObss.length > 0) {
        throw new Error("should not happen");
      }
    }),
  );
}
