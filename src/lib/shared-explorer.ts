import axios from "axios";
import { backOff } from "exponential-backoff";
import Client from "ioredis";
import Redlock from "redlock";
import { Chain } from "../types/chain";
import { sleep } from "../utils/async";
import {
  EXPLORER_URLS,
  LOG_LEVEL,
  MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS,
  REDIS_URL,
} from "../utils/config";
import { logger } from "../utils/logger";

let client: Client | null = null;
async function getRedisClient() {
  if (!client) {
    client = new Client(REDIS_URL);
  }
  return client;
}

let redlock: Redlock | null = null;
async function getRedlock() {
  if (!redlock) {
    const client = await getRedisClient();
    redlock = new Redlock(
      // You should have one client for each independent redis node
      // or cluster.
      [client],
      {
        // The expected clock drift; for more details see:
        // http://redis.io/topics/distlock
        driftFactor: 0.01, // multiplied by lock ttl to determine drift time

        // The max number of times Redlock will attempt to lock a resource
        // before erroring.
        retryCount: 10,

        // the time in ms between attempts
        retryDelay: 1000, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter: 200, // time in ms

        // The minimum remaining time on a lock before an extension is automatically
        // attempted with the `using` API.
        automaticExtensionThreshold: 10_000, // time in ms
      }
    );
  }
  return redlock;
}

export async function callLockProtectedExplorerUrl<TRes>(
  chain: Chain,
  params: Record<string, string>
) {
  const client = await getRedisClient();
  const redlock = await getRedlock();
  // first, try to acquire a lock as there could be only one call at any time per explorer on the whole system
  const explorerResourceId = `${chain}:explorer:lock`;
  const explorerUrl = EXPLORER_URLS[chain];
  const delayBetweenCalls = MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS[chain];

  logger.debug(`[EXPLORER] Trying to acquire lock for ${explorerResourceId}`);
  // do multiple tries as well
  return backOff(
    () =>
      redlock.using([explorerResourceId], 60 * 1000, async () => {
        logger.verbose(`[EXPLORER] Acquired lock for ${explorerResourceId}`);
        // now, we are the only one running this code
        // find out the last time we called this explorer
        const lastCallCacheKey = `${chain}:explorer:last-call-date`;
        const lastCallStr = await client.get(lastCallCacheKey);
        const lastCallDate =
          lastCallStr && lastCallStr !== ""
            ? new Date(lastCallStr)
            : new Date(0);

        const now = new Date();

        // wait a bit before calling the explorer again
        if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
          logger.debug(
            `[EXPLORER] Last call too close for ${explorerUrl}, sleeping a bit`
          );
          await sleep(delayBetweenCalls);
        }
        const url = explorerUrl + "?" + new URLSearchParams(params).toString();

        // now we are going to call, so set the last call date
        await client.set(lastCallCacheKey, now.toISOString());

        const res = await axios.get<
          | { status: "0"; message: string; result: string }
          | { status: "0"; message: "No records found"; result: [] }
          | { status: "1"; result: TRes }
        >(url);

        if (
          res.data.status === "0" &&
          res.data.message === "No records found"
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
      maxDelay: 60 * 1000,
      numOfAttempts: 10,
      retry: (error, attemptNumber) => {
        logger.error(
          `[EXPLORER] Error on attempt ${attemptNumber} calling explorer for ${explorerUrl}: ${error.message}`
        );
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
