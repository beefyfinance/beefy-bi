import * as Rx from "rxjs";
import { rootLogger } from "../../logger";
import { ProgrammerError } from "./programmer-error";

const logger = rootLogger.child({ module: "rxjs-utils", component: "buffer-until-below-machine-threshold" });

export function bufferUntilBelowMachineThresholds<TObj>(options: {
  cpuSamplingIntervalMs: number;
  maxCpuThresholdPercent: number;
  maxMemoryThresholdMb: number;
  checkIntervalMs: number;
}) {
  if (options.maxCpuThresholdPercent < 0 || options.maxCpuThresholdPercent > 100) {
    throw new ProgrammerError("maxCpuThresholdPercent must be between 0 and 100");
  }
  if (options.maxMemoryThresholdMb < 0) {
    throw new ProgrammerError("maxMemoryThresholdMb must be positive");
  }
  if (options.cpuSamplingIntervalMs < 0) {
    throw new ProgrammerError("cpuSamplingIntervalMs must be positive");
  }
  if (options.cpuSamplingIntervalMs > 10_000) {
    throw new ProgrammerError("cpuSamplingIntervalMs must be less than 10s");
  }

  // poll cpu usage
  let lastCpu = { start: Date.now(), stop: Date.now(), usage: process.cpuUsage() };
  let cpuPolling = setInterval(() => {
    const newUsage = process.cpuUsage(lastCpu.usage);
    lastCpu = { start: lastCpu.stop, stop: Date.now(), usage: newUsage };
  }, options.cpuSamplingIntervalMs);

  function isBelowThreshold() {
    const dateDelta = lastCpu.stop - lastCpu.start; // in 10^-3 seconds
    const userDelta = lastCpu.usage.user; // in 10^-6 seconds
    const dateDeltaSec = dateDelta / 1_000;
    const userDeltaSec = userDelta / 1_000_000;
    const cpuPercent = (userDeltaSec / dateDeltaSec) * 100;

    const memoryRss = process.memoryUsage.rss();
    const memoryMb = memoryRss / 1024.0 / 1024.0;

    const logData = {
      cpu: { dateDelta, userDelta, dateDeltaSec, userDeltaSec, cpuPercent },
      memory: { memoryRss, memoryMb },
      options,
      lastCpu,
    };

    if (cpuPercent < options.maxCpuThresholdPercent && memoryMb < options.maxMemoryThresholdMb) {
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
        clearInterval(cpuPolling);
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
