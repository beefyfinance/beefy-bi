import * as Rx from "rxjs";
import { rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "rxjs-utils", component: "buffer-until-key-changed" });

export function bufferUntilKeyChanged<TObj>(options: {
  getKey: (obj: TObj) => string;
  maxBufferSize: number;
  maxBufferTimeMs: number;
  pollFrequencyMs: number;
  pollJitterMs: number;
  logInfos: { msg: string; data?: Record<string, unknown> };
}): Rx.OperatorFunction<TObj, TObj[]> {
  let objBuffer = [] as TObj[];

  let sourceObsFinalized = false;

  function flushBuffer() {
    // find how much we can send, but always send at least one
    // until key changed or max buffer size reached

    let previousKey = null;
    let i = 0;
    for (; i < objBuffer.length && i < options.maxBufferSize; i++) {
      const obj = objBuffer[i];
      const objKey = options.getKey(obj);
      if (previousKey === null) {
        previousKey = objKey;
      }
      // we have reached the end of the current key
      if (objKey !== previousKey) {
        break;
      }
    }
    // send the first item anyway if it is too big
    if (i === 0) {
      i = 1;
    }
    const toSend = objBuffer.slice(0, i);
    objBuffer = objBuffer.slice(i);

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

      // send the buffer if we have reached the buffer size
      if (objBuffer.length >= options.maxBufferSize) {
        const toSend = flushBuffer();
        subscriber.next(() => Rx.of(toSend));
        return;
      }
    }, options.pollFrequencyMs + Math.random() * options.pollJitterMs);

    return () => {
      logger.trace({ msg: "Releaser unsubscribed" });
      clearInterval(poller);
    };
  });

  return Rx.pipe(
    // queue up items as they come in
    Rx.map((obj: TObj) => {
      objBuffer.push(obj);
      return () => Rx.EMPTY as Rx.Observable<TObj[]>;
    }),

    // register the finalization of the source obs
    Rx.finalize(() => {
      logger.trace({ msg: "Source obs finalized" });
      sourceObsFinalized = true;
    }),

    // replace them with our releaser
    Rx.mergeWith(releaser$),

    Rx.map((fn) => fn()),
    Rx.mergeAll(),
    Rx.filter((objs) => objs.length > 0),
  );
}
