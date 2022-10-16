import * as Rx from "rxjs";
import { createObservableWithNext } from "./create-observable-with-next";

export function bufferUntilCountReached<TObj>(options: {
  getCount: (obj: TObj) => number;
  maxBufferSize: number;
  maxBufferTimeMs: number;
  pollFrequencyMs: number;
  pollJitterMs: number;
  logInfos: { msg: string; data?: Record<string, unknown> };
}): Rx.OperatorFunction<TObj, TObj[]> {
  const { next, complete, observable } = createObservableWithNext();
  let total = 0;
  let lastEmit: null | number = null;

  const poller = setInterval(() => {
    if (lastEmit === null) {
      return;
    }

    const now = Date.now();
    const elapsed = now - lastEmit;
    if (elapsed >= options.maxBufferTimeMs) {
      lastEmit = null;
      total = 0;
      next(1);
    }
  }, options.pollFrequencyMs + Math.random() * options.pollJitterMs);

  return Rx.pipe(
    Rx.tap({
      next: (obj) => {
        if (lastEmit === null) {
          lastEmit = Date.now();
        }

        const count = options.getCount(obj);
        total += count;
        if (total > options.maxBufferSize) {
          total = count;
          lastEmit = null;
          next(1);
        }
        return obj;
      },
    }),

    Rx.bufferWhen(() => observable),

    Rx.filter((objs) => objs.length > 0),

    Rx.finalize(() => {
      clearInterval(poller);
      complete();
    }),
  );
}
