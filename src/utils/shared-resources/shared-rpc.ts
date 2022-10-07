import { backOff } from "exponential-backoff";
import { Chain } from "../../types/chain";
import { sleep } from "../../utils/async";
import { getRedisClient, getRedlock } from "./shared-lock";
import { ArchiveNodeNeededError } from "../rpc/archive-node-needed";
import { rootLogger } from "../../utils/logger";
import { RpcLimitations } from "../rpc/rpc-limitations";
import { ethers } from "ethers";
import { removeSecretsFromRpcUrl } from "../rpc/remove-secrets-from-rpc-url";
import { ProgrammerError } from "../rxjs/utils/programmer-error";

const logger = rootLogger.child({ module: "shared-resources", component: "rpc-lock" });

export async function callLockProtectedRpc<TRes>(
  chain: Chain,
  provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider,
  rpcLimitations: RpcLimitations,
  work: () => Promise<TRes>,
) {
  const client = await getRedisClient();
  const redlock = await getRedlock();
  const delayBetweenCalls = rpcLimitations.minDelayBetweenCalls;

  // create a loggable string as raw rpc url may contain an api key
  const publicRpcUrl = removeSecretsFromRpcUrl(provider.connection.url);
  const rpcLockId = `${chain}:rpc:lock:${publicRpcUrl}`;
  const lastCallCacheKey = `${chain}:rpc:last-call-date:${publicRpcUrl}`;

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
        return doWork();
      } else {
        logger.trace({ msg: "Trying to acquire lock", data: { publicRpcUrl, resourceId: rpcLockId } });
        return redlock.using([rpcLockId], 2 * 60 * 1000, async () => {
          logger.trace({ msg: " Acquired lock for", data: { publicRpcUrl, resourceId: rpcLockId } });
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
          data: { attemptNumber, publicRpcUrl, resourceId: rpcLockId, error: error.message },
        };
        if (attemptNumber < 3) logger.trace(message);
        else if (attemptNumber < 9) logger.info(message);
        else if (attemptNumber < 10) logger.warn(message);
        else logger.error(message);

        // some errors are not recoverable
        if (error instanceof ArchiveNodeNeededError) {
          return false;
        }
        if (error instanceof ProgrammerError) {
          return false;
        }
        return true;
      },
      startingDelay: delayBetweenCalls !== "no-limit" ? delayBetweenCalls : 0,
      timeMultiple: 5,
    },
  );
}
