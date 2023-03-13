import { RateLimitOptions, RateLimitPluginOptions } from "@fastify/rate-limit";
import { getRedisClient } from "../../utils/shared-resources/shared-lock";

export async function getRateLimitOpts() {
  const redisClient = await getRedisClient();
  const opts: RateLimitOptions & RateLimitPluginOptions = {
    max: 5,
    timeWindow: 5000,
    cache: 10000,
    redis: redisClient,
    continueExceeding: true,
    skipOnError: false,
    enableDraftSpec: true, // default false. Uses IEFT draft header standard

    // don't cache 429s
    onExceeding: function (req, key) {
      req.headers["cache-control"] = "no-cache";
    },
  };
  return opts;
}
