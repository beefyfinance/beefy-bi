import Decimal from "decimal.js";
import { DbClient, db_query } from "../../utils/db";
import { ProgrammerError } from "../../utils/programmer-error";
import { TimeBucket, timeBucketToSamplingPeriod } from "../schema/time-bucket";
import { AsyncCache } from "./cache";

export class PriceService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getPriceTs(priceFeedId: number, timeBucket: TimeBucket) {
    const { timeRange } = timeBucketToSamplingPeriod(timeBucket);

    const cacheKey = `api:price-service:simple:${priceFeedId}-${timeBucket}`;
    const ttl = 1000 * 60 * 5; // 5 min

    const matViewMap: { [key in TimeBucket]: string } = {
      "1h_1d": "price_ts_cagg_1h",
      "1h_1w": "price_ts_cagg_1h",
      "1d_1M": "price_ts_cagg_1d",
      "1d_1Y": "price_ts_cagg_1d",
    };
    const matView = matViewMap[timeBucket];
    if (!matView) {
      throw new ProgrammerError("Unsupported time bucket: " + timeBucket);
    }

    const result = await this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<{
        datetime: string;
        price_avg: string;
        price_high: string;
        price_low: string;
        price_open: string;
        price_close: string;
      }>(
        `
        SELECT 
          datetime as datetime, 
          price_avg,
          price_high,
          price_low,
          price_open,
          price_close
        FROM ${matView}
        WHERE price_feed_id = %L
          AND datetime > NOW() - %L::INTERVAL
        order by 1 asc
      `,
        [priceFeedId, timeRange],
        this.services.db,
      ),
    );
    return result.map((row) => ({
      datetime: new Date(row.datetime),
      price_avg: new Decimal(row.price_avg),
      price_high: new Decimal(row.price_high),
      price_low: new Decimal(row.price_low),
      price_open: new Decimal(row.price_open),
      price_close: new Decimal(row.price_close),
    }));
  }

  async getLastPrices(priceFeedIds: number[]): Promise<Map<number, Decimal>> {
    if (priceFeedIds.length === 0) return new Map();

    const cacheKey = `api:price-service:last-price:${priceFeedIds.join(",")}`;
    const ttl = 1000 * 60 * 5; // 5 min

    const result = await this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<{ price_feed_id: number; price: string }>(
        `
        SELECT 
          price_feed_id,
          last(price, datetime) as price
        FROM price_ts
        WHERE price_feed_id IN (%L)
        group by 1
      `,
        [priceFeedIds],
        this.services.db,
      ),
    );
    return new Map(result.map((row) => [row.price_feed_id, new Decimal(row.price)]));
  }
}
