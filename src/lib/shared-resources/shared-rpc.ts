import { ethers } from "ethers";
import { backOff } from "exponential-backoff";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import {
  LOG_LEVEL,
  MIN_DELAY_BETWEEN_RPC_CALLS_MS,
  RPC_BACH_CALL_COUNT,
  RPC_URLS,
} from "../../utils/config";
import { logger } from "../../utils/logger";
import { getRedisClient, getRedlock } from "./shared-lock";
import * as lodash from "lodash";

export async function callLockProtectedRpc<TRes>(
  chain: Chain,
  work: (provider: ethers.providers.JsonRpcProvider) => Promise<TRes>
) {
  const client = await getRedisClient();
  const redlock = await getRedlock();
  const delayBetweenCalls = MIN_DELAY_BETWEEN_RPC_CALLS_MS[chain];

  // create a loggable string as raw rpc url may contain an api key
  const secretRpcUrl = lodash.sample(RPC_URLS[chain]) as string; // pick one at random
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
        const lastCallDate =
          lastCallStr && lastCallStr !== ""
            ? new Date(lastCallStr)
            : new Date(0);

        const now = new Date();
        logger.debug(
          `[RPC] Last call was ${lastCallDate.toISOString()} (now: ${now.toISOString()})`
        );

        // wait a bit before calling the rpc again if needed
        if (delayBetweenCalls !== "no-limit") {
          if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
            logger.debug(
              `[RPC] Last call too close for ${publicRpcUrl}, sleeping a bit`
            );
            await sleep(delayBetweenCalls);
            logger.debug(`[RPC] Resuming call to ${publicRpcUrl}`);
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
        logger.debug(`[RPC] No lock needed for ${publicRpcUrl}`);
        return doWork();
      } else {
        logger.debug(
          `[RPC] Trying to acquire lock for ${resourceId} (${publicRpcUrl})`
        );
        return redlock.using([resourceId], 2 * 60 * 1000, async () => {
          logger.debug(
            `[RPC] Acquired lock for ${resourceId} (${publicRpcUrl})`
          );
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
        const message = `[RPC] Error on attempt ${attemptNumber} calling rpc for ${publicRpcUrl}: ${error.message}`;
        if (attemptNumber < 3) logger.verbose(message);
        else if (attemptNumber < 5) logger.info(message);
        else if (attemptNumber < 8) logger.warn(message);
        else logger.error(message);

        if (LOG_LEVEL === "trace") {
          console.error(error);
        }
        // some errors are not recoverable
        if (error instanceof ArchiveNodeNeededError) {
          return false;
        }
        return true;
      },
      startingDelay: delayBetweenCalls !== "no-limit" ? delayBetweenCalls : 0,
      timeMultiple: 5,
    }
  );
}

export class ArchiveNodeNeededError extends Error {
  constructor(public readonly chain: Chain, public readonly error: any) {
    super(`Archive node needed for ${chain}`);
  }
}
export function isErrorDueToMissingDataFromNode(error: any) {
  // parse from ehter-wrapped rpc calls
  const errorRpcBody = lodash.get(error, "error.body");
  if (errorRpcBody && lodash.isString(errorRpcBody)) {
    const rpcBodyError = JSON.parse(errorRpcBody);
    const errorCode = lodash.get(rpcBodyError, "error.code");
    const errorMessage = lodash.get(rpcBodyError, "error.message");

    if (
      errorCode === -32000 &&
      lodash.isString(errorMessage) &&
      (errorMessage.includes("Run with --pruning=archive") ||
        // Cf: https://github.com/ethereum/go-ethereum/issues/20557
        errorMessage.startsWith("missing trie node"))
    ) {
      return true;
    }
  }

  // also parse from direct rpc responses
  const directRpcError = lodash.get(error, "error");
  if (
    directRpcError &&
    lodash.isObjectLike(directRpcError) &&
    lodash.get(directRpcError, "code") === -32000 &&
    lodash.get(directRpcError, "message")?.startsWith("missing trie node")
  ) {
    return true;
  }
  return false;
}
