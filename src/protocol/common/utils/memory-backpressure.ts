import * as Rx from "rxjs";
import { BACKPRESSURE_CHECK_INTERVAL_MS, BACKPRESSURE_MEMORY_THRESHOLD_MB } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { looselessThrottleWhen } from "../../../utils/rxjs/utils/looseless-throttle-when";

const logger = rootLogger.child({ module: "rxjs-utils", component: "memory-backpressure" });

/**
 * Throttle input objects until the process memory is below the configured threshold.
 */
export function memoryBackpressure$<TObj>(options: { logInfos: { msg: string; data?: object }; sendBurstsOf: number }) {
  return Rx.pipe(
    looselessThrottleWhen<TObj>({
      checkIntervalJitterMs: 200,
      checkIntervalMs: BACKPRESSURE_CHECK_INTERVAL_MS,
      logInfos: options.logInfos,
      shouldSend: () => {
        const memoryMb = getProcessMemoryMb();

        if (memoryMb < BACKPRESSURE_MEMORY_THRESHOLD_MB) {
          logger.trace({ msg: "Sending in buffered item. " + options.logInfos.msg, data: { ...options.logInfos.data, ...options } });
          return options.sendBurstsOf;
        }
        logger.trace({ msg: "Buffering until below machine thresholds. " + options.logInfos.msg, data: { ...options.logInfos.data, ...options } });
        return 0;
      },
    }),
  );
}

function getProcessMemoryMb() {
  const memoryRss = process.memoryUsage.rss();
  const memoryMb = memoryRss / 1024.0 / 1024.0;
  return memoryMb;
}
