import * as Rx from "rxjs";
import { BACKPRESSURE_CHECK_INTERVAL_MS, BACKPRESSURE_CHECK_JITTER_MS, BACKPRESSURE_MEMORY_THRESHOLD_MB } from "../../../utils/config";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { throttleWhen } from "../../../utils/rxjs/utils/throttle-when";

const logger = rootLogger.child({ module: "rxjs-utils", component: "memory-backpressure" });

/**
 * Throttle input objects until the process memory is below the configured threshold.
 */
export function memoryBackpressure$<TObj>(options: { logInfos: LogInfos; sendBurstsOf: number }) {
  return Rx.pipe(
    throttleWhen<TObj>({
      checkIntervalJitterMs: BACKPRESSURE_CHECK_JITTER_MS,
      checkIntervalMs: BACKPRESSURE_CHECK_INTERVAL_MS,
      logInfos: options.logInfos,
      sendBurstsOf: options.sendBurstsOf,
      shouldSend: () => {
        const memoryMb = getProcessMemoryMb();

        if (memoryMb < BACKPRESSURE_MEMORY_THRESHOLD_MB) {
          logger.trace(mergeLogsInfos({ msg: "Sending in buffered item", data: { sendBurstsOf: options.sendBurstsOf } }, options.logInfos));
          return true;
        }
        logger.trace(
          mergeLogsInfos({ msg: "Buffering until below machine thresholds", data: { sendBurstsOf: options.sendBurstsOf } }, options.logInfos),
        );
        return false;
      },
    }),
  );
}

function getProcessMemoryMb() {
  const memoryRss = process.memoryUsage.rss();
  const memoryMb = memoryRss / 1024.0 / 1024.0;
  return memoryMb;
}
