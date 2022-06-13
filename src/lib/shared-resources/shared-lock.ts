import Client from "ioredis";
import Redlock from "redlock";
import { REDIS_URL } from "../../utils/config";
import { logger } from "../../utils/logger";

let client: Client | null = null;
export async function getRedisClient() {
  if (!client) {
    logger.verbose(`[LOCK] Creating redis client`);
    client = new Client(REDIS_URL);
  }
  return client;
}

let redlock: Redlock | null = null;
export async function getRedlock() {
  if (!redlock) {
    logger.verbose(`[LOCK] Creating redlock client`);
    const client = await getRedisClient();
    redlock = new Redlock(
      // You should have one client for each independent redis node
      // or cluster.
      [client],
      {
        // The expected clock drift; for more details see:
        // http://redis.io/topics/distlock
        driftFactor: 0.01, // multiplied by lock ttl to determine drift time

        // The max number of times Redlock will attempt to lock a resource
        // before erroring.
        retryCount: 10,

        // the time in ms between attempts
        retryDelay: 3000, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter: 1000, // time in ms

        // The minimum remaining time on a lock before an extension is automatically
        // attempted with the `using` API.
        automaticExtensionThreshold: 60_000, // time in ms
      }
    );
  }
  return redlock;
}