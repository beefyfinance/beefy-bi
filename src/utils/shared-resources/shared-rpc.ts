import AsyncLock from "async-lock";
import { ethers } from "ethers";
import { backOff } from "exponential-backoff";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../utils/logger";
import { ProgrammerError } from "../programmer-error";
import { isErrorRetryable } from "../retryable-error";
import { removeSecretsFromRpcUrl } from "../rpc/remove-secrets-from-rpc-url";
import { RpcLimitations } from "../rpc/rpc-limitations";
import { getRedisClient, getRedlock } from "./shared-lock";

const logger = rootLogger.child({ module: "shared-resources", component: "rpc-lock" });

const chainLock = new AsyncLock({
  // max amount of time an item can remain in the queue before acquiring the lock
  timeout: 0, // never
  // we don't want a lock to be reentered
  domainReentrant: false,
  //max amount of time allowed between entering the queue and completing execution
  maxOccupationTime: 0, // never
  // max number of tasks allowed in the queue at a time
  maxPending: 100_000,
});

export async function callLockProtectedRpc<TRes>(
  work: () => Promise<TRes>,
  options: {
    maxTotalRetryMs: number;
    logInfos: LogInfos;
    chain: Chain;
    provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider;
    rpcLimitations: RpcLimitations;
  },
) {
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

  const client = await getRedisClient();
  const redlock = await getRedlock();
  const delayBetweenCalls = options.rpcLimitations.minDelayBetweenCalls;

  // create a loggable string as raw rpc url may contain an api key
  const publicRpcUrl = removeSecretsFromRpcUrl(options.provider.connection.url);
  const rpcLockId = `${options.chain}:rpc:lock:${publicRpcUrl}`;
  const lastCallCacheKey = `${options.chain}:rpc:last-call-date:${publicRpcUrl}`;

  // do multiple tries as well
  return backOff(
    () => {
      const doWork = async () => {
        // now, we are the only one running this code
        // find out the last time we called this explorer
        const lastCallStr = await client.get(lastCallCacheKey);
        const lastCallDate = lastCallStr && lastCallStr !== "" ? new Date(lastCallStr) : new Date(0);

        const now = new Date();
        logger.trace({
          msg: "Last call was",
          data: { lastCallDate: lastCallDate.toISOString(), now: now.toISOString() },
        });

        // wait a bit before calling the rpc again if needed
        if (delayBetweenCalls !== "no-limit") {
          if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
            logger.trace({ msg: "Last call too close, sleeping a bit", data: { publicRpcUrl, resourceId: rpcLockId } });
            await sleep(delayBetweenCalls);
            logger.trace({ msg: "Resuming call to rpc", data: { publicRpcUrl, resourceId: rpcLockId } });
          }
        }
        // now we are going to call, so set the last call date
        await client.set(lastCallCacheKey, new Date().toISOString());

        let res: TRes | null = null;
        try {
          res = await work();
        } finally {
          // reset the last call date if the call succeeded
          // just in case rate limiting is accounted by explorers at the end of requests
          await client.set(lastCallCacheKey, new Date().toISOString());
        }

        return res;
      };

      if (delayBetweenCalls === "no-limit") {
        logger.trace({ msg: "No lock needed for", data: { publicRpcUrl, resourceId: rpcLockId } });

        // acquire a local lock for this chain so in case we do use batch provider, we are guaranteed
        // the we are only batching the current request size and not more
        return chainLock.acquire(options.chain, doWork);
      } else {
        logger.trace({ msg: "Trying to acquire lock", data: { publicRpcUrl, resourceId: rpcLockId } });
        return redlock.using([rpcLockId], 2 * 60 * 1000, async () => {
          logger.trace({ msg: " Acquired lock for", data: { publicRpcUrl, resourceId: rpcLockId } });

          // acquire a local lock for this chain so in case we do use batch provider, we are guaranteed
          // the we are only batching the current request size and not more
          return chainLock.acquire(options.chain, doWork);
        });
      }
    },
    {
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
        } else {
          logger.debug(mergeLogsInfos({ msg: "Unretryable RPC Error caught, will not retry", data: { error } }, options.logInfos));
          logger.error(error);
        }
        return isRetryable;
      },
    },
  );
}
