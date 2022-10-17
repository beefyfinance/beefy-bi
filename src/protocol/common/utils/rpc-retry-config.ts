import { IBackOffOptions } from "exponential-backoff";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { isErrorRetryable } from "../../../utils/retryable-error";

const logger = rootLogger.child({ module: "rpc", component: "retry" });

export function getRpcRetryConfig(options: { maxTotalRetryMs: number; logInfos: { msg: string; data?: object } }): Partial<IBackOffOptions> {
  const startingDelay = 100;
  const timeMultiple = 5;

  // get the total attempt number to reach the max delay
  let numOfAttempts = 0;
  let currentDelay = startingDelay;
  let totalWait = 0;
  while (totalWait < options.maxTotalRetryMs) {
    numOfAttempts++;
    totalWait += currentDelay;
    currentDelay *= timeMultiple;
  }
  // we want to wait strictly less than the max delay
  numOfAttempts--;

  if (numOfAttempts < 1) {
    throw new ProgrammerError(
      mergeLogsInfos(
        {
          msg: "Invalid retry configuration",
          data: { totalAttempt: numOfAttempts, startingDelay, timeMultiple, maxTotalRetryMs: options.maxTotalRetryMs },
        },
        options.logInfos,
      ),
    );
  }

  logger.trace(
    mergeLogsInfos(
      {
        msg: "RPC retry config",
        data: { totalAttempt: numOfAttempts, startingDelay, timeMultiple, maxTotalRetryMs: options.maxTotalRetryMs },
      },
      options.logInfos,
    ),
  );

  return {
    delayFirstAttempt: false,
    startingDelay,
    timeMultiple,
    maxDelay: options.maxTotalRetryMs,
    numOfAttempts,

    jitter: "full",
    retry: (error, attemptNumber) => {
      const isRetryable = isErrorRetryable(error);
      if (isRetryable) {
        const logMsg = mergeLogsInfos({ msg: "RPC Error caught, will retry", data: { error } }, options.logInfos);
        if (attemptNumber < 3) logger.trace(logMsg);
        else if (attemptNumber < 5) logger.debug(logMsg);
        else if (attemptNumber < 9) logger.info(logMsg);
        else if (attemptNumber < 10) logger.warn(logMsg);
        else {
          logger.error(logMsg);
          logger.error(error);
        }
        //console.log(error);
      } else {
        logger.debug(mergeLogsInfos({ msg: "Unretryable RPC Error caught, will not retry", data: { error } }, options.logInfos));
        logger.error(error);
      }
      return isRetryable;
    },
  };
}
