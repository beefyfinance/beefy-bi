import { getRedisClient } from "../../utils/shared-resources/shared-lock";

export async function getRateLimitOpts() {
  const redisClient = await getRedisClient();
  return {
    max: 1,
    timeWindow: 5000,
    cache: 10000,
    redis: redisClient,
    continueExceeding: true,
    skipOnError: false,
    enableDraftSpec: true, // default false. Uses IEFT draft header standard
  };
}
