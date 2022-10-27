import { AbstractCacheCompliantObject } from "@fastify/caching";

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
    const cached = await this.get<{ item: T; stored: number; ttl: number }>(key);
    if (cached) {
      return cached.item;
    }
    const value = await fn();
    await this.set(key, value, ttlMs);
    return value;
  }
}
