import { RateLimitOptions } from "@fastify/rate-limit";

export async function getRateLimitOpts(): RateLimitOptions {
  return {
    max: 5,
    timeWindow: 5000,
    cache: 10000,
    continueExceeding: true,
    skipOnError: false,
    enableDraftSpec: true, // default false. Uses IEFT draft header standard

    // don't cache 429s
    onExceeding: function (req, key) {
      req.headers["cache-control"] = "no-cache";
    },
  };
}
