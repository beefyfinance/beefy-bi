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
      "1h_1M": "price_ts_cagg_1h",
      "4h_3M": "VIRTUAL_price_ts_cagg_4h",
      "1d_1M": "price_ts_cagg_1d",
      "1d_1Y": "price_ts_cagg_1d",
      "1d_all": "price_ts_cagg_1d",
    };
    const matView = matViewMap[timeBucket];
    if (!matView) {
      throw new ProgrammerError("Unsupported time bucket: " + timeBucket);
    }

    const result = await this.services.cache.wrap(cacheKey, ttl, async () => {
      type Ret = {
        datetime: string;
        price_avg: string;
        price_high: string;
        price_low: string;
        price_open: string;
        price_close: string;
      };
      // this table does not exists, we fake it
      if (matView === "VIRTUAL_price_ts_cagg_4h") {
        return db_query<Ret>(
          `
          SELECT 
            time_bucket('4 hour', datetime) as datetime,
            AVG(price_avg) as price_avg, -- absolutely incorrect, but good enough
            MAX(price_high) as price_high,
            MIN(price_low) as price_low,
            FIRST(price_open, datetime) as price_open,
            LAST(price_close, datetime) as price_close
          FROM price_ts_cagg_1h
          WHERE price_feed_id = %L
            AND datetime > NOW() - %L::INTERVAL
          group by 1
          order by 1 asc
        `,
          [priceFeedId, timeRange],
          this.services.db,
        );
      }
      return db_query<Ret>(
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
      );
    });
    return result.map((row) => ({
      datetime: new Date(row.datetime),
      price_avg: row.price_avg,
      price_high: row.price_high,
      price_low: row.price_low,
      price_open: row.price_open,
      price_close: row.price_close,
    }));
  }
}
