import * as Rx from "rxjs";
import { createObservableWithNext } from "./create-observable-with-next";

export function bufferUntilKeyChanged<TObj>(options: {
  getKey: (obj: TObj) => string;
  maxBufferSize: number;
  maxBufferTimeMs: number;
  pollFrequencyMs: number;
  pollJitterMs: number;
  logInfos: { msg: string; data?: Record<string, unknown> };
}): Rx.OperatorFunction<TObj, TObj[]> {
  const { next, complete, observable } = createObservableWithNext();
  let bufferSize = 0;
  let lastKey: string | null = null;
  let lastEmit: null | number = null;

  const poller = setInterval(() => {
    if (lastEmit === null) {
      return;
    }

    const now = Date.now();
    const elapsed = now - lastEmit;

    if (elapsed >= options.maxBufferTimeMs) {
      lastEmit = null;
      lastKey = null;
      bufferSize = 0;
      next(1);
    }
  }, options.pollFrequencyMs + Math.random() * options.pollJitterMs);

  return Rx.pipe(
    Rx.tap({
      next: (obj) => {
        if (lastEmit === null) {
          lastEmit = Date.now();
        }

        const key = options.getKey(obj);

        if (key === null) {
          lastKey = key;
          bufferSize++;
        } else if (key !== lastKey) {
          lastKey = key;
          lastEmit = null;
          bufferSize = 1;
          next(1);
        } else if (bufferSize >= options.maxBufferSize) {
          lastEmit = null;
          bufferSize = 1;
          next(1);
        } else {
          bufferSize++;
        }
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
