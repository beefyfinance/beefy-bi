import { getRedisClient } from "../../utils/shared-resources/shared-lock";

export async function getRateLimitOpts() {
  const redisClient = await getRedisClient();
  return {
    max: 5,
    timeWindow: 5000,
    cache: 10000,
    redis: redisClient,
    continueExceeding: true,
    skipOnError: false,
    enableDraftSpec: true, // default false. Uses IEFT draft header standard
    addHeaders: {
      // default config
      "x-ratelimit-limit": true,
      "x-ratelimit-remaining": true,
      "x-ratelimit-reset": true,
      "retry-after": true,
      // avoid caching 429s
      "cache-control": "no-cache",
    },
  };
}
