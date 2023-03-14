import { RateLimitOptions } from "@fastify/rate-limit";

export async function getRateLimitOpts(): Promise<RateLimitOptions> {
  return {
    //max: 5,
    max: 25,
    timeWindow: 5000,
    cache: 10000,
    continueExceeding: true,
    skipOnError: false,
    enableDraftSpec: true, // default false. Uses IEFT draft header standard
  };
}
