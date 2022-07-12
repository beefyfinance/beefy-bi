import axios from "axios";
import { backOff } from "exponential-backoff";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import {
  EXPLORER_URLS,
  LOG_LEVEL,
  MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS,
} from "../../utils/config";
import { logger } from "../../utils/logger";
import { getRedisClient, getRedlock } from "./shared-lock";

export async function callLockProtectedExplorerUrl<TRes>(
  chain: Chain,
  params: Record<string, string>
) {
  const client = await getRedisClient();
  const redlock = await getRedlock();
  // first, try to acquire a lock as there could be only one call at any time per explorer on the whole system
  const explorerResourceId = `${chain}:explorer:lock`;
  const explorerUrl = EXPLORER_URLS[chain];
  const delayBetweenCalls = MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS;

  logger.debug(`[EXPLORER] Trying to acquire lock for ${explorerResourceId}`);
  // do multiple tries as well
  return backOff(
    () =>
      redlock.using([explorerResourceId], 2 * 60 * 1000, async () => {
        logger.debug(`[EXPLORER] Acquired lock for ${explorerResourceId}`);
        // now, we are the only one running this code
        // find out the last time we called this explorer
        const lastCallCacheKey = `${chain}:explorer:last-call-date`;
        const lastCallStr = await client.get(lastCallCacheKey);
        const lastCallDate =
          lastCallStr && lastCallStr !== ""
            ? new Date(lastCallStr)
            : new Date(0);

        const now = new Date();
        logger.debug(
          `[EXPLORER] Last call was ${lastCallDate.toISOString()} (now: ${now.toISOString()})`
        );

        // wait a bit before calling the explorer again
        if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
          logger.debug(
            `[EXPLORER] Last call too close for ${explorerUrl}, sleeping a bit`
          );
          await sleep(delayBetweenCalls);
          logger.debug(`[EXPLORER] Resuming call to ${explorerUrl}`);
        }
        const url = explorerUrl + "?" + new URLSearchParams(params).toString();
        logger.debug(`[EXPLORER] Calling ${url}`);

        // now we are going to call, so set the last call date
        await client.set(lastCallCacheKey, new Date().toISOString());

        const res = await axios.get<
          | { status: "0"; message: string; result: string }
          | { status: "0"; message: "No records found"; result: [] }
          | { status: "0"; message: "No logs found"; result: [] }
          | { status: "1"; result: TRes }
        >(url);

        // reset the last call date if the call succeeded
        // just in case rate limiting is accounted by explorers at the end of requests
        await client.set(lastCallCacheKey, new Date().toISOString());

        if (
          res.data.status === "0" &&
          (res.data.message === "No records found" ||
            res.data.message === "No logs found" ||
            res.data.message === "No transactions found")
        ) {
          return [];
        }

        if (res.data.status === "0") {
          logger.error(
            `[EXPLORER] Error calling explorer ${explorerUrl}: ${JSON.stringify(
              res.data
            )}`
          );
          throw new Error(`[EXPLORER] Explorer call failed: ${explorerUrl}`);
        }
        return res.data.result;
      }),
    {
      delayFirstAttempt: false,
      jitter: "full",
      maxDelay: 5 * 60 * 1000,
      numOfAttempts: 10,
      retry: (error, attemptNumber) => {
        const message = `[EXPLORER] Error on attempt ${attemptNumber} calling explorer for ${explorerUrl}: ${error.message}`;
        if (attemptNumber < 3) logger.verbose(message);
        else if (attemptNumber < 5) logger.info(message);
        else if (attemptNumber < 8) logger.warn(message);
        else logger.error(message);

        if (LOG_LEVEL === "trace") {
          console.error(error);
        }
        return true;
      },
      startingDelay: delayBetweenCalls,
      timeMultiple: 2,
    }
  );
}

// some errors will not be retried, so we need to handle them separately
export class ExplorerQuerySpansTooMuchData extends Error {
  constructor(message: string) {
    super(message);
  }
}
