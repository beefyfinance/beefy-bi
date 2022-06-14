import * as redis from "redis";
import { REDIS_URL } from "./config";
import { logger } from "./logger";

// I can't believe this doesn't exists already
class RedisCache {
  constructor(protected redisClient: redis.RedisClientType) {}

  public async has(key: string): Promise<boolean> {
    const res = await this.redisClient.exists(key);
    return res === 1;
  }

  public async getJSON<ReturnType>(key: string): Promise<ReturnType | null> {
    const resStr = await this.redisClient.get(key);
    if (resStr === null) {
      return null;
    }
    const res: ReturnType = JSON.parse(resStr);
    return res;
  }

  public async setJSON(
    key: string,
    value: any,
    ttl_sec?: number
  ): Promise<void> {
    await this.redisClient.set(key, JSON.stringify(value), { EX: ttl_sec });
  }
}

let cache: RedisCache | null = null;
async function getCache() {
  if (cache === null) {
    const client = redis.createClient({ url: REDIS_URL });
    client.on("error", (error) => {
      if (error) {
        logger.error("[REDIS] client error", error);
      } else {
        logger.info("[REDIS] client connected");
      }
    });
    await client.connect();
    // @ts-ignore idk why typescript is screaming here
    cache = new RedisCache(client);
  }
  return cache;
}

export function cacheAsyncResultInRedis<
  TArgs extends any[],
  TReturn extends object
>(
  fn: (...parameters: TArgs) => Promise<TReturn>,
  options: {
    getKey: (...params: TArgs) => string;
    dateFields?: (keyof TReturn)[];
    ttl_sec?: number;
  }
): (...parameters: TArgs) => Promise<TReturn> {
  const dateFields = options?.dateFields || [];
  const ttl_sec = options?.ttl_sec || undefined;

  const cachedFn = async function (...args: any[]): Promise<TReturn> {
    const cache = await getCache();
    // @ts-ignore
    const key = options.getKey(...args);
    if (await cache.has(key)) {
      //logger.debug(`[REDIS] Cache hit for ${key}`);
      const obj = await cache.getJSON<TReturn>(key);
      for (const field of dateFields) {
        // @ts-ignore
        obj[field] = new Date(obj[field]);
      }
      // @ts-ignore
      return obj;
    }
    logger.verbose(`[REDIS] Cache miss for ${key}`);
    // @ts-ignore
    const res: TReturn = await fn(...args);
    await cache.setJSON(key, res, ttl_sec);
    return res;
  };
  Object.defineProperty(cachedFn, "name", {
    value: "cached_" + fn.name,
    writable: false,
  });
  return cachedFn;
}
