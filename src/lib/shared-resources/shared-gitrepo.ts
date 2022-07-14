import { backOff } from "exponential-backoff";
import { LOG_LEVEL } from "../../utils/config";
import { logger } from "../../utils/logger";
import { getRedlock } from "./shared-lock";

export async function callLockProtectedGitRepo(repo: string, work: () => Promise<any>) {
  const redlock = await getRedlock();
  // first, try to acquire a lock as there could be only one call at any time per explorer on the whole system
  const resourceId = `git:repo:${repo}:lock`;

  logger.debug(`[GIT.LOCK] Trying to acquire lock for ${resourceId}`);
  // do multiple tries as well
  return backOff(
    () =>
      redlock.using([resourceId], 2 * 60 * 1000, async () => {
        logger.debug(`[GIT.LOCK] Acquired lock for ${resourceId}`);

        const res = await work();
        return res;
      }),
    {
      delayFirstAttempt: false,
      jitter: "full",
      maxDelay: 5 * 60 * 1000,
      numOfAttempts: 10,
      retry: (error, attemptNumber) => {
        const message = `[GIT.LOCK] Error on attempt ${attemptNumber} calling explorer for ${resourceId}: ${error.message}`;
        if (attemptNumber < 3) logger.debug(message);
        else if (attemptNumber < 5) logger.verbose(message);
        else if (attemptNumber < 8) logger.info(message);
        else if (attemptNumber < 9) logger.warn(message);
        else logger.error(message);

        if (LOG_LEVEL === "trace") {
          console.error(error);
        }
        return true;
      },
      startingDelay: 0,
      timeMultiple: 2,
    }
  );
}
