import { AbstractCacheCompliantObject } from "@fastify/caching";
import { rootLogger } from "../../utils/logger";

const logger = rootLogger.child({ module: "api", component: "cache" });

export class AsyncCache {
  constructor(protected services: { abcache: AbstractCacheCompliantObject }) {}

  async get<T>(key: string): Promise<T> {
    return new Promise((resolve, reject) => {
      this.services.abcache.get(key, (err, value) => {
        if (err) {
          reject(err);
        } else {
          resolve(value as T);
        }
      });
    });
  }

  async set<T>(key: string, value: T, ttlMs: number): Promise<T> {
    return new Promise((resolve, reject) => {
      this.services.abcache.set(key, value, ttlMs, (err, value) => {
        if (err) {
          reject(err);
        } else {
          resolve(value as T);
        }
      });
    });
  }

  async wrap<T>(key: string, ttlMs: number, fn: () => Promise<T>): Promise<T> {
    logger.debug({ msg: "wrap: called", data: { key, ttlMs } });
    const cached = await this.get<{ item: T; stored: number; ttl: number }>(key);
    if (cached) {
      logger.trace({ msg: "wrap: cache hit", data: { key, ttlMs } });
      return cached.item;
    }
    logger.trace({ msg: "wrap: cache miss", data: { key, ttlMs } });
    const value = await fn();
    if (value !== null && value !== undefined) {
      logger.trace({ msg: "wrap: setting non null cache value", data: { key, ttlMs } });
      await this.set(key, value, ttlMs);
      logger.trace({ msg: "wrap: cache value set", data: { key, ttlMs } });
    }
    return value;
  }
}
