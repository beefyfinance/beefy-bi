import { IBackOffOptions } from "exponential-backoff";
import { shouldRetryRpcError } from "../../../lib/rpc/archive-node-needed";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError, shouldRetryProgrammerError } from "../../../utils/rxjs/utils/programmer-error";

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
    throw new ProgrammerError({
      msg: "Invalid retry configuration",
      data: { totalAttempt: numOfAttempts, startingDelay, timeMultiple, maxTotalRetryMs: options.maxTotalRetryMs },
    });
  }

  logger.trace({
    msg: `RPC retry config: ${options.logInfos.msg}`,
    data: { ...options.logInfos.data, totalAttempt: numOfAttempts, startingDelay, timeMultiple, maxTotalRetryMs: options.maxTotalRetryMs },
  });

  return {
    delayFirstAttempt: false,
    startingDelay,
    timeMultiple,
    maxDelay: options.maxTotalRetryMs,
    numOfAttempts,

    jitter: "full",
    retry: (error, attemptNumber) => {
      const shouldRetry = shouldRetryRpcError(error) && shouldRetryProgrammerError(error);
      if (shouldRetry) {
        const logMsg = { msg: `RPC Error caught, will retry: ${options.logInfos.msg}`, data: { ...options.logInfos.data, error } };
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
        logger.debug({ msg: `Unretryable error caught, will not retry: ${options.logInfos.msg}`, data: { ...options.logInfos.data, error } });
        logger.error(error);
      }
      return shouldRetry;
    },
  };
}
