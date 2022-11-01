import { DbClient, db_query } from "../../utils/db";
import { AsyncCache } from "./cache";

export class PriceService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getPriceTs(priceFeedId: number) {
    const cacheKey = `api:price-service:${priceFeedId}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<{
        price_feed_1_id: number;
        price_feed_2_id: number;
      }>(
        `
        SELECT datetime, price 
        FROM price_ts
        WHERE price_feed_id = %L
          AND datetime > NOW() - INTERVAL '7 days'
      `,
        [priceFeedId],
        this.services.db,
      ),
    );
  }
}
