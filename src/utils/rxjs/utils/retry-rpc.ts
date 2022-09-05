import { retryBackoff } from "backoff-rxjs";
import { shouldRetryRpcError } from "../../../lib/rpc/archive-node-needed";
import { rootLogger } from "../../logger2";
import { shouldRetryProgrammerError } from "./programmer-error";

const logger = rootLogger.child({ module: "rpc", component: "retry" });

export function retryRpcErrors(logInfos: object) {
  return retryBackoff({
    initialInterval: 100,
    maxInterval: 10_000,
    maxRetries: 12,
    // 👇 resets retries count and delays between them to init values
    resetOnSuccess: true,
    shouldRetry: (err: any) => {
      const shouldRetry = shouldRetryRpcError(err) && shouldRetryProgrammerError(err);
      if (shouldRetry) {
        logger.error({ msg: "RPC Error caught, will retry", data: { ...logInfos, error: err } });
        logger.error(err);
      } else {
        logger.debug({ msg: "Unretryable error caught, will not retry", data: { ...logInfos, error: err } });
        logger.trace(err);
      }
      return shouldRetry;
    },
    backoffDelay: (iteration: number, initialInterval: number) => {
      logger.debug({ msg: "retrying rpc call", data: { iteration, initialInterval, logInfos } });
      return Math.pow(2, iteration) * initialInterval;
    },
  });
}
