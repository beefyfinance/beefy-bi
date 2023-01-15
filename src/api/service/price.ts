import { SamplingPeriod } from "../../types/sampling";
import { DbClient, db_query } from "../../utils/db";
import { assertIsValidTimeBucket } from "../schema/time-bucket";
import { AsyncCache } from "./cache";

export class PriceService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getPriceTs(priceFeedId: number, bucketSize: SamplingPeriod, timeRange: SamplingPeriod) {
    // only accept some parameter combination
    assertIsValidTimeBucket(bucketSize, timeRange);

    const cacheKey = `api:price-service:simple:${priceFeedId}-${bucketSize}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<{
        datetime: string;
        price: string;
      }>(
        `
        SELECT 
          time_bucket(%L, datetime) as datetime, 
          AVG(price) as price_avg,
          MAX(price) as price_high,
          MIN(price) as price_low,
          FIRST(price, datetime) as price_open,
          LAST(price, datetime) as price_close
        FROM price_ts
        WHERE price_feed_id = %L
          AND datetime > NOW() - %L::INTERVAL
        group by 1
        order by 1 asc
      `,
        [bucketSize, priceFeedId, timeRange],
        this.services.db,
      ),
    );
  }
}
