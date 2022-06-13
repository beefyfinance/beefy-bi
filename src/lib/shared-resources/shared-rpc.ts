import { ethers } from "ethers";
import { backOff } from "exponential-backoff";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import {
  LOG_LEVEL,
  MIN_DELAY_BETWEEN_RPC_CALLS_MS,
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
  const rpcUrl = lodash.sample(RPC_URLS[chain]) as string; // pick one at random
  const rpcIndex = RPC_URLS[chain].indexOf(rpcUrl);
  const resourceId = `${chain}:rpc:${rpcIndex}:lock`;
  const delayBetweenCalls = MIN_DELAY_BETWEEN_RPC_CALLS_MS;

  logger.debug(
    `[EXPLORER] Trying to acquire lock for ${resourceId} (${rpcUrl})`
  );
  // do multiple tries as well
  return backOff(
    () =>
      redlock.using([resourceId], 2 * 60 * 1000, async () => {
        logger.verbose(
          `[EXPLORER] Acquired lock for ${resourceId} (${rpcUrl})`
        );
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
          `[EXPLORER] Last call was ${lastCallDate.toISOString()} (now: ${now.toISOString()})`
        );

        // wait a bit before calling the explorer again
        if (now.getTime() - lastCallDate.getTime() < delayBetweenCalls) {
          logger.debug(
            `[EXPLORER] Last call too close for ${rpcUrl}, sleeping a bit`
          );
          await sleep(delayBetweenCalls);
          logger.debug(`[EXPLORER] Resuming call to ${rpcUrl}`);
        }
        // now we are going to call, so set the last call date
        await client.set(lastCallCacheKey, new Date().toISOString());

        const provider = new ethers.providers.JsonRpcProvider(rpcUrl);

        let res: TRes | null = null;
        try {
          res = await work(provider);
        } catch (error) {
          const errorRpcBody = lodash.get(error, "error.body");
          if (errorRpcBody && lodash.isString(errorRpcBody)) {
            const blockNumber = 0;
            const rpcBodyError = JSON.parse(errorRpcBody);
            const errorCode = lodash.get(rpcBodyError, "error.code");
            const errorMessage = lodash.get(rpcBodyError, "error.message");
            const errorReason = lodash.get(error, "reason");
            if (
              errorCode === -32000 &&
              ((lodash.isString(errorMessage) &&
                errorMessage.startsWith(
                  "missing revert data in call exception"
                )) ||
                (lodash.isString(errorReason) &&
                  errorReason.startsWith(
                    "missing revert data in call exception;"
                  )))
            ) {
              throw new ArchiveNodeNeededError(chain, blockNumber, error);
            }
          }
          throw error;
        }

        // reset the last call date if the call succeeded
        // just in case rate limiting is accounted by explorers at the end of requests
        await client.set(lastCallCacheKey, new Date().toISOString());

        return res;
      }),
    {
      delayFirstAttempt: false,
      jitter: "full",
      maxDelay: 5 * 60 * 1000,
      numOfAttempts: 10,
      retry: (error, attemptNumber) => {
        logger.error(
          `[EXPLORER] Error on attempt ${attemptNumber} calling rpc for ${rpcUrl}: ${error.message}`
        );
        if (LOG_LEVEL === "trace") {
          console.error(error);
        }
        // some errors are not recoverable
        if (error instanceof ArchiveNodeNeededError) {
          return false;
        }
        return true;
      },
      startingDelay: delayBetweenCalls,
      timeMultiple: 5,
    }
  );
}

export class ArchiveNodeNeededError extends Error {
  constructor(
    public readonly chain: Chain,
    public readonly blockNumber: number,
    public readonly error: any
  ) {
    super(`Archive node needed for ${chain}:${blockNumber}`);
  }
}
