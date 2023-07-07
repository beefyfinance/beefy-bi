import AsyncLock from "async-lock";
import { ethers } from "ethers";
import { backOff, IBackOffOptions } from "exponential-backoff";
import { isString } from "lodash";
import { createHash } from "node:crypto";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../utils/logger";
import { ProgrammerError } from "../programmer-error";
import { isErrorRetryable } from "../retryable-error";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../rpc/archive-node-needed";
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

type CallLockProtectedRpcOptions = {
  maxTotalRetryMs: number;
  logInfos: LogInfos;
  chain: Chain;
  provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider | ethers.providers.EtherscanProvider | string;
  rpcLimitations: RpcLimitations;
  // if true, we will not call the work function behind a AsyncLock
  // it's up to the caller to ensure the work function is properly batched
  // AsyncLock is used to ensure we are batching only the calls made in the work function
  // this is a limitation of the batching design of ethersjs which relies on nodejs event loop instead of a proper batch object
  noLockIfNoLimit: boolean;
};

const rpcPublicUniqueIds = new Map<string, string>();
function getRpcPublicUniqueId(chain: Chain, rpcUrl: string): string {
  if (!rpcPublicUniqueIds.has(rpcUrl)) {
    const publicRpcUrl = removeSecretsFromRpcUrl(chain, rpcUrl);
    const hash = createHash("md5").update(rpcUrl).digest("hex");
    rpcPublicUniqueIds.set(rpcUrl, `${publicRpcUrl}:${hash}`);
  }
  return rpcPublicUniqueIds.get(rpcUrl)!;
}

export async function callLockProtectedRpc<TRes>(work: () => Promise<TRes>, options: CallLockProtectedRpcOptions) {
  if (options.rpcLimitations.minDelayBetweenCalls === "no-limit") {
    return callNoLimitRpc(work, options);
  }

  const delayBetweenCalls = options.rpcLimitations.minDelayBetweenCalls;
  const lastCallDateCache = await getRedisClient();
  const redlock = await getRedlock();

  // create a string we can log as raw rpc url may contain an api key
  const url =
    options.provider instanceof ethers.providers.EtherscanProvider
      ? options.provider.getBaseUrl()
      : isString(options.provider)
      ? options.provider
      : options.provider.connection.url;
  const publicRpcUrl = removeSecretsFromRpcUrl(options.chain, url);
  const rpcLockId = `${options.chain}:rpc:lock:${getRpcPublicUniqueId(options.chain, url)}`;
  const lastCallCacheKey = `${options.chain}:rpc:last-call-date:${getRpcPublicUniqueId(options.chain, url)}`;

  async function waitUntilWeCanCallRPCAgain() {
    // find out the last time we called this explorer
    const lastCallStr = await lastCallDateCache.get(lastCallCacheKey);
    const lastCallDate = lastCallStr && lastCallStr !== "" ? new Date(lastCallStr) : new Date(0);

    const now = new Date();
    logger.trace({
      msg: "Last call was",
      data: { lastCallDate: lastCallDate.toISOString(), now: now.toISOString() },
    });

    // wait a bit before calling the rpc again if needed
    if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
      logger.trace(mergeLogsInfos({ msg: "Last call too close, sleeping a bit", data: { publicRpcUrl, resourceId: rpcLockId } }, options.logInfos));
      await sleep(delayBetweenCalls);
      logger.trace(mergeLogsInfos({ msg: "Resuming call to rpc", data: { publicRpcUrl, resourceId: rpcLockId } }, options.logInfos));
    }
  }

  async function workAndSetLastCall() {
    // now, we are the only one running this code
    await waitUntilWeCanCallRPCAgain();

    // now we are going to call, so set the last call date
    await lastCallDateCache.set(lastCallCacheKey, new Date().toISOString());

    let res: TRes | null = null;
    try {
      res = await work();
    } catch (error) {
      if (error instanceof ArchiveNodeNeededError) {
        throw error;
      } else if (isErrorDueToMissingDataFromNode(error)) {
        throw new ArchiveNodeNeededError(options.chain, error);
      }
      throw error;
    } finally {
      // reset the last call date if the call succeeded
      // just in case rate limiting is accounted by explorers at the end of requests
      await lastCallDateCache.set(lastCallCacheKey, new Date().toISOString());
    }

    return res;
  }

  // do multiple tries as well
  const callWithBackoff = () =>
    backOff(async () => {
      let res: TRes;
      // do a sleep session before acquiring the lock if needed
      await waitUntilWeCanCallRPCAgain();

      logger.trace(mergeLogsInfos({ msg: "Trying to acquire lock", data: { publicRpcUrl, resourceId: rpcLockId } }, options.logInfos));
      let lock = await redlock.acquire([rpcLockId], 2 * 60 * 1000);
      logger.trace(mergeLogsInfos({ msg: "Lock acquired", data: { publicRpcUrl, resourceId: rpcLockId } }, options.logInfos));
      try {
        res = await workAndSetLastCall();
      } finally {
        logger.trace(mergeLogsInfos({ msg: "Releasing lock", data: { publicRpcUrl, resourceId: rpcLockId } }, options.logInfos));
        await lock.release();
      }
      logger.trace(mergeLogsInfos({ msg: "RPC Call success", data: { publicRpcUrl, resourceId: rpcLockId } }, options.logInfos));
      return res;
    }, getBackoffConfiguration(options));

  try {
    logger.trace({ msg: "Calling RPC", data: { publicRpcUrl, resourceId: rpcLockId } });
    const res = await callWithBackoff();
    logger.trace(mergeLogsInfos({ msg: "RPC call succeeded", data: { publicRpcUrl } }, options.logInfos));
    return res;
  } catch (error) {
    logger.error(mergeLogsInfos({ msg: "RPC call failed", data: { publicRpcUrl, error } }, options.logInfos));
    logger.error(error);
    throw error;
  }
}

/**
 * Rpcs in no-limit more are not protected by a lock at all
 */
async function callNoLimitRpc<TRes>(work: () => Promise<TRes>, options: CallLockProtectedRpcOptions) {
  // create a string we can log as raw rpc url may contain an api key
  const url =
    options.provider instanceof ethers.providers.EtherscanProvider
      ? options.provider.getBaseUrl()
      : isString(options.provider)
      ? options.provider
      : options.provider.connection.url;
  const publicRpcUrl = removeSecretsFromRpcUrl(options.chain, url);
  const rpcLockId = `${options.chain}:rpc:lock:${publicRpcUrl}`;

  async function workBehindLockIfNeeded() {
    if (options.noLockIfNoLimit) {
      return work();
    } else {
      // acquire a local lock for this chain so in case we do use batch provider, we are guaranteed
      // the we are only batching the current request size and not more
      return chainLock.acquire(rpcLockId, work);
    }
  }

  try {
    logger.trace({ msg: "Calling RPC", data: { publicRpcUrl, resourceId: rpcLockId } });
    const res = await backOff(workBehindLockIfNeeded);
    logger.trace(mergeLogsInfos({ msg: "RPC call succeeded", data: { publicRpcUrl } }, options.logInfos));
    return res;
  } catch (error) {
    logger.error(mergeLogsInfos({ msg: "RPC call failed", data: { publicRpcUrl, error } }, options.logInfos));
    logger.error(error);
    throw error;
  }
}

function getBackoffConfiguration(options: CallLockProtectedRpcOptions): Partial<IBackOffOptions> {
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
  const backoffConfig: Partial<IBackOffOptions> = {
    delayFirstAttempt: false,
    startingDelay,
    timeMultiple,
    maxDelay: options.maxTotalRetryMs,
    numOfAttempts,
    jitter: "full",
    retry: (error, attemptNumber) => {
      const isRetryable = isErrorRetryable(error);
      if (isRetryable) {
        const logMsg = mergeLogsInfos({ msg: "RPC Error caught, will retry", data: { error, attemptNumber, numOfAttempts } }, options.logInfos);
        if (attemptNumber < 3) logger.trace(logMsg);
        else if (attemptNumber < 5) logger.debug(logMsg);
        else if (attemptNumber < 9) logger.info(logMsg);
        else if (attemptNumber < 10) logger.warn(logMsg);
        else {
          logger.error(logMsg);
          logger.error(error);
        }
      } else {
        logger.debug(
          mergeLogsInfos({ msg: "Unretryable RPC Error caught, will not retry", data: { error, attemptNumber, numOfAttempts } }, options.logInfos),
        );
        logger.error(error);
      }
      return isRetryable;
    },
  };
  return backoffConfig;
}
