import * as Rx from "rxjs";
import { rootLogger } from "../../logger";
import { ProgrammerError } from "./programmer-error";

const logger = rootLogger.child({ module: "rxjs-utils", component: "buffer-until-below-machine-threshold" });

export function bufferUntilBelowMachineThresholds<TObj>(options: { maxMemoryThresholdMb: number; checkIntervalMs: number }) {
  if (options.maxMemoryThresholdMb < 0) {
    throw new ProgrammerError("maxMemoryThresholdMb must be positive");
  }

  function isBelowThreshold() {
    const memoryRss = process.memoryUsage.rss();
    const memoryMb = memoryRss / 1024.0 / 1024.0;

    const logData = {
      memory: { memoryRss, memoryMb },
      options,
    };

    if (memoryMb < options.maxMemoryThresholdMb) {
      logger.trace({ msg: "Sending in buffered item", data: logData });
      return true;
    }
    logger.trace({ msg: "Buffering until below machine thresholds", data: logData });
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
        const obj = objQueue.shift()!;
        subscriber.next(() => Rx.of(obj));
      }

      // close if we are done
      if (sourceObsFinalized && objQueue.length <= 0) {
        // clear all intervals to release memory
        clearInterval(poller);
        subscriber.complete();
      }
    }, options.checkIntervalMs);
  });

  return Rx.pipe(
    // queue up items as they come in
    Rx.map((obj: TObj) => {
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
