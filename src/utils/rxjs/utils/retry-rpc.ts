import { retryBackoff } from "backoff-rxjs";
import { shouldRetryRpcError } from "../../../lib/rpc/archive-node-needed";
import { rootLogger } from "../../logger2";

const logger = rootLogger.child({ module: "rpc", component: "retry" });

export function retryRpcErrors(logInfos: object) {
  return retryBackoff({
    initialInterval: 100,
    maxInterval: 10_000,
    maxRetries: 12,
    // 👇 resets retries count and delays between them to init values
    resetOnSuccess: true,
    shouldRetry: shouldRetryRpcError,
    backoffDelay: (iteration: number, initialInterval: number) => {
      logger.error({ msg: "retrying rpc call", data: { iteration, initialInterval, logInfos } });
      return Math.pow(2, iteration) * initialInterval;
    },
  });
}
