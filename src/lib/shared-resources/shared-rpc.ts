import { ethers } from "ethers";
import { backOff } from "exponential-backoff";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import { MIN_DELAY_BETWEEN_RPC_CALLS_MS, RPC_BACH_CALL_COUNT, RPC_URLS } from "../../utils/config";
import { getRedisClient, getRedlock } from "./shared-lock";
import { rootLogger } from "../../utils/logger2";
import { get, isObjectLike, isString, sample } from "lodash";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../rpc/archive-node-needed";

const logger = rootLogger.child({ module: "shared-resources", component: "rpc-lock" });

export async function callLockProtectedRpc<TRes>(
  chain: Chain,
  work: (provider: ethers.providers.JsonRpcProvider) => Promise<TRes>,
) {
  const client = await getRedisClient();
  const redlock = await getRedlock();
  const delayBetweenCalls = MIN_DELAY_BETWEEN_RPC_CALLS_MS[chain];

  // create a loggable string as raw rpc url may contain an api key
  const secretRpcUrl = sample(RPC_URLS[chain]) as string; // pick one at random
  const urlObj = new URL(secretRpcUrl);
  const publicRpcUrl = urlObj.protocol + "//" + urlObj.hostname;
  const rpcIndex = RPC_URLS[chain].indexOf(secretRpcUrl);
  const resourceId = `${chain}:rpc:${rpcIndex}:lock`;

  // do multiple tries as well
  return backOff(
    () => {
      const doWork = async () => {
        // now, we are the only one running this code
        // find out the last time we called this explorer
        const lastCallCacheKey = `${chain}:rpc:${rpcIndex}:last-call-date`;
        const lastCallStr = await client.get(lastCallCacheKey);
        const lastCallDate = lastCallStr && lastCallStr !== "" ? new Date(lastCallStr) : new Date(0);

        const now = new Date();
        logger.debug({
          msg: "Last call was",
          data: { lastCallDate: lastCallDate.toISOString(), now: now.toISOString() },
        });

        // wait a bit before calling the rpc again if needed
        if (delayBetweenCalls !== "no-limit") {
          if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
            logger.debug({ msg: "Last call too close, sleeping a bit", data: { publicRpcUrl, resourceId } });
            await sleep(delayBetweenCalls);
            logger.debug({ msg: "Resuming call to rpc", data: { publicRpcUrl, resourceId } });
          }
        }
        // now we are going to call, so set the last call date
        await client.set(lastCallCacheKey, new Date().toISOString());

        // cronos throws a typeerror when called with a batch provider, and we can't catch those so it crashed the script
        const provider =
          RPC_BACH_CALL_COUNT[chain] === "no-batching"
            ? new ethers.providers.JsonRpcProvider(secretRpcUrl)
            : new ethers.providers.JsonRpcBatchProvider(secretRpcUrl);
        /*
        provider.on("debug", (event) => {
          if (event.action === "response") {
            console.log(event);
          }
        });*/

        let res: TRes | null = null;
        try {
          res = await work(provider);
        } catch (error) {
          if (error instanceof ArchiveNodeNeededError) {
            throw error;
          } else if (isErrorDueToMissingDataFromNode(error)) {
            throw new ArchiveNodeNeededError(chain, error);
          }
          throw error;
        }

        // reset the last call date if the call succeeded
        // just in case rate limiting is accounted by explorers at the end of requests
        await client.set(lastCallCacheKey, new Date().toISOString());

        return res;
      };

      if (delayBetweenCalls === "no-limit") {
        logger.debug({ msg: "No lock needed for", data: { publicRpcUrl, resourceId } });
        return doWork();
      } else {
        logger.debug({ msg: "Trying to acquire lock", data: { publicRpcUrl, resourceId } });
        return redlock.using([resourceId], 2 * 60 * 1000, async () => {
          logger.debug({ msg: " Acquired lock for", data: { publicRpcUrl, resourceId } });
          return doWork();
        });
      }
    },
    {
      delayFirstAttempt: false,
      jitter: "full",
      maxDelay: 5 * 60 * 1000,
      numOfAttempts: 10,
      retry: (error, attemptNumber) => {
        const message = {
          msg: "RPC error, retrying",
          data: { attemptNumber, publicRpcUrl, resourceId, error: error.message },
        };
        if (attemptNumber < 3) logger.debug(message);
        else if (attemptNumber < 9) logger.info(message);
        else if (attemptNumber < 10) logger.warn(message);
        else logger.error(message);

        // some errors are not recoverable
        if (error instanceof ArchiveNodeNeededError) {
          return false;
        }
        return true;
      },
      startingDelay: delayBetweenCalls !== "no-limit" ? delayBetweenCalls : 0,
      timeMultiple: 5,
    },
  );
}
