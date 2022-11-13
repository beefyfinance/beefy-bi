import { SamplingPeriod } from "../../types/sampling";
import { DbClient, db_query } from "../../utils/db";
import { AsyncCache } from "./cache";

export class PriceService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getPriceTs(priceFeedId: number, bucketSize: SamplingPeriod) {
    const cacheKey = `api:price-service:simple:${priceFeedId}-${bucketSize}`;
    const ttl = 1000 * 60 * 5; // 5 min
    const pointsToFetch = 1000;
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<{
        datetime: string;
        price: string;
      }>(
        `
        SELECT 
          time_bucket(%L, datetime) as datetime, 
          last(price, datetime) as price
        FROM price_ts
        WHERE price_feed_id = %L
          AND datetime > NOW() - (%L::INTERVAL * %L)
        group by 1
        order by 1 asc
      `,
        [bucketSize, priceFeedId, bucketSize, pointsToFetch],
        this.services.db,
      ),
    );
  }

  async getPriceOhlcTs(priceFeedId: number, bucketSize: SamplingPeriod) {
    const cacheKey = `api:price-service:ohlc:${priceFeedId}-${bucketSize}`;
    const ttl = 1000 * 60 * 5; // 5 min
    const pointsToFetch = 1000;
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<{
        datetime: string;
        open: string;
        high: string;
        low: string;
        close: string;
      }>(
        `
          SELECT 
            time_bucket(%L, datetime) as datetime, 
            first(price, datetime) as open,
            max(price) as high,
            min(price) as low,
            last(price, datetime) as close
          FROM price_ts
          WHERE price_feed_id = %L
            AND datetime > NOW() - (%L::INTERVAL * %L)
          group by 1
          order by 1 asc
      `,
        [bucketSize, priceFeedId, bucketSize, pointsToFetch],
        this.services.db,
      ),
    );
  }
}
