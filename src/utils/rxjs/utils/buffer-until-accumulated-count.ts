import { sum } from "lodash";
import * as Rx from "rxjs";
import { rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "rxjs-utils", component: "buffer-until-accumulated-count" });

export function bufferUntilAccumulatedCountReached<TObj>(options: {
  getCount: (obj: TObj) => number;
  maxBufferSize: number;
  maxBufferTimeMs: number;
  pollFrequencyMs: number;
  pollJitterMs: number;
  logInfos: { msg: string; data?: Record<string, unknown> };
}): Rx.OperatorFunction<TObj, TObj[]> {
  let objBuffer = [] as TObj[];
  let bufferTotal = 0;

  let sourceObsFinalized = false;

  function flushBuffer() {
    // find how much we can send
    let count = 0;
    let i = 0;
    for (; i < objBuffer.length; i++) {
      const obj = objBuffer[i];
      const objCount = options.getCount(obj);
      if (count + objCount > options.maxBufferSize) {
        break;
      }
      count += objCount;
    }
    // send the first item anyway if it is too big
    if (i === 0) {
      i = 1;
    }
    const toSend = objBuffer.slice(0, i);
    objBuffer = objBuffer.slice(i);

    // reset the buffer total
    bufferTotal = sum(objBuffer.map(options.getCount));

    return toSend;
  }

  const releaser$ = new Rx.Observable<() => Rx.Observable<TObj[]>>((subscriber) => {
    const lastSentAt = Date.now();

    const poller = setInterval(() => {
      // no work
      if (objBuffer.length <= 0) {
        // here we are fully done
        if (sourceObsFinalized) {
          subscriber.complete();
        }
        return;
      }

      // send the buffer if we have reached the time limit
      const now = Date.now();
      if (now - lastSentAt > options.maxBufferTimeMs) {
        const toSend = flushBuffer();
        subscriber.next(() => Rx.of(toSend));
        return;
      }

      // send the buffer if we have reached the count limit
      if (bufferTotal >= options.maxBufferSize) {
        const toSend = flushBuffer();
        subscriber.next(() => Rx.of(toSend));
        return;
      }
    }, options.pollFrequencyMs + Math.random() * options.pollJitterMs);

    return () => {
      logger.trace({
        msg: "Releaser unsubscribed. " + options.logInfos.msg,
        data: {
          ...options.logInfos.data,
          maxBufferSize: options.maxBufferSize,
          maxBufferTimeMs: options.maxBufferTimeMs,
          pollFrequencyMs: options.pollFrequencyMs,
          pollJitterMs: options.pollJitterMs,
        },
      });
      clearInterval(poller);
    };
  });

  return Rx.pipe(
    // queue up items as they come in
    Rx.map((obj: TObj) => {
      objBuffer.push(obj);
      bufferTotal += options.getCount(obj);
      return () => Rx.EMPTY as Rx.Observable<TObj[]>;
    }),

    // register the finalization of the source obs
    Rx.finalize(() => {
      logger.trace({
        msg: "Source obs finalized. " + options.logInfos.msg,
        data: {
          ...options.logInfos.data,
          maxBufferSize: options.maxBufferSize,
          maxBufferTimeMs: options.maxBufferTimeMs,
          pollFrequencyMs: options.pollFrequencyMs,
          pollJitterMs: options.pollJitterMs,
        },
      });
      sourceObsFinalized = true;
    }),

    // replace them with our releaser
    Rx.mergeWith(releaser$),

    Rx.map((fn) => fn()),
    Rx.mergeAll(),
    Rx.filter((objs) => objs.length > 0),
  );
}
