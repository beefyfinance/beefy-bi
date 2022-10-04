import * as Rx from "rxjs";
import { rootLogger } from "../../logger";
import { ProgrammerError } from "./programmer-error";

const logger = rootLogger.child({ module: "rxjs-utils", component: "buffer-until-below-machine-threshold" });

export function bufferUntilBelowMachineThresholds<TObj>(options: {
  sendInitialBurstOf: number; // ramp up to speed quickly by sending a large burst
  checkIntervalMs: number; // then every X ms
  checkIntervalJitterMs: number; // add a bit of jitter to the check interval
  maxMemoryThresholdMb: number; // check memory usage
  sendByBurstOf: number; // and send a burst to be processed if memory is below threshold
}) {
  if (options.maxMemoryThresholdMb < 0) {
    throw new ProgrammerError("maxMemoryThresholdMb must be positive");
  }

  function isBelowThreshold() {
    const memoryRss = process.memoryUsage.rss();
    const memoryMb = memoryRss / 1024.0 / 1024.0;

    const logData = {
      memory: { memoryRss, memoryMb },
      options,
      queueSize: objQueue.length,
    };

    if (memoryMb < options.maxMemoryThresholdMb) {
      if (objQueue.length > 0) {
        logger.trace({ msg: "Sending in buffered item", data: logData });
      }
      return true;
    }
    if (objQueue.length > 0) {
      logger.trace({ msg: "Buffering until below machine thresholds", data: logData });
    }
    return false;
  }

  const objQueue: TObj[] = [];
  let sourceObsFinalized = false;

  const releaser$ = new Rx.Observable<() => Rx.Observable<TObj>>((subscriber) => {
    const poller = setInterval(() => {
      // no work
      if (objQueue.length <= 0) {
        return;
      }

      // check if we are below threshold
      if (isBelowThreshold()) {
        for (let i = 0; i < options.sendByBurstOf; i++) {
          const obj = objQueue.shift();
          if (obj === undefined) {
            break;
          }
          subscriber.next(() => Rx.of(obj));
        }
      }

      // close if we are done
      if (sourceObsFinalized && objQueue.length <= 0) {
        // clear all intervals to release memory
        clearInterval(poller);
        subscriber.complete();
      }
    }, options.checkIntervalMs + Math.random() * options.checkIntervalJitterMs);
  });

  let remainingInitialBurst = options.sendInitialBurstOf;

  return Rx.pipe(
    // queue up items as they come in
    Rx.map((obj: TObj) => {
      if (remainingInitialBurst > 0) {
        // except if we already crossed the line
        if (!isBelowThreshold()) {
          logger.trace({ msg: "Threshold crossed while sending initial burst of items", data: { remainingInitialBurst } });
          remainingInitialBurst = 0;
          objQueue.push(obj);
          return () => Rx.EMPTY;
        }
        remainingInitialBurst--;
        logger.trace({ msg: "Sending initial burst of items", data: { remainingInitialBurst } });
        return () => Rx.of(obj);
      }
      objQueue.push(obj);
      return () => Rx.EMPTY as Rx.Observable<TObj>;
    }),
    // register the finalization of the source obs
    Rx.finalize(() => {
      logger.trace({ msg: "Source obs finalized" });
      sourceObsFinalized = true;
    }),

    // replace them with our releaser
    Rx.mergeWith(releaser$),

    // ensure we sent all objects
    Rx.map((fn) => fn()),
    Rx.concatAll(),
  );
}
