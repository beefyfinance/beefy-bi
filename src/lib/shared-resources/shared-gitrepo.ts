import { backOff } from "exponential-backoff";
import { getRedlock } from "./shared-lock";
import { rootLogger } from "../../utils/logger";

const logger = rootLogger.child({ module: "shared-resources", component: "git-local-repo-lock" });

export async function callLockProtectedGitRepo(repo: string, work: () => Promise<any>) {
  const redlock = await getRedlock();
  // first, try to acquire a lock as there could be only one call at any time per explorer on the whole system
  const resourceId = `git:repo:${repo}:lock`;

  logger.debug({ msg: "Trying to acquire lock", data: { resourceId } });
  // do multiple tries as well
  return backOff(
    () =>
      redlock.using([resourceId], 2 * 60 * 1000, async () => {
        logger.debug({ msg: "Acquired lock", data: { resourceId } });

        const res = await work();
        return res;
      }),
    {
      delayFirstAttempt: false,
      jitter: "full",
      maxDelay: 5 * 60 * 1000,
      numOfAttempts: 10,
      retry: (error, attemptNumber) => {
        const message = {
          msg: "Error on attempt, retrying",
          data: { resourceId, attemptNumber, error: error.message },
        };
        if (attemptNumber < 3) logger.debug(message);
        else if (attemptNumber < 9) logger.info(message);
        else if (attemptNumber < 10) logger.warn(message);
        else logger.error(message);

        return true;
      },
      startingDelay: 0,
      timeMultiple: 2,
    },
  );
}
