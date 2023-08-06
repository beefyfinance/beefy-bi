import { SamplingPeriod } from "../../types/sampling";
import { DbClient, db_query, db_query_one } from "../../utils/db";
import { ProgrammerError } from "../../utils/programmer-error";
import { PriceType } from "../schema/price-type";
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

  async getRawPriceTs(priceFeedId: number, fromInclusive: Date, toExclusive: Date, limit: number) {
    type Ret = {
      datetime: Date;
      block_number: number;
      price: string;
    };
    const result = await db_query<Ret>(
      `
        SELECT 
          datetime,
          block_number,
          price
        FROM price_ts
        WHERE price_feed_id = %L
          AND datetime >= %L AND datetime < %L
        ORDER BY 1 asc
        LIMIT %L
        `,
      [priceFeedId, fromInclusive, toExclusive, limit],
      this.services.db,
    );

    return result.map((row) => ({
      datetime: new Date(row.datetime),
      block_number: row.block_number,
      price: row.price,
    }));
  }

  public static pricesAroundADateResponseSchema = {
    description: "List of 2*`half_limit` prices closest to the `utc_datetime`, for the given `oracle_id` and `look_around` period.",
    type: "object",
    properties: {
      priceFeed: {
        type: "object",
        properties: {
          price_feed_id: { type: "number", description: "The price feed id" },
          feed_key: { type: "string", description: "The price feed key" },
        },
        required: ["price_feed_id", "feed_key"],
      },
      priceRows: {
        type: "array",
        items: {
          type: "object",
          properties: {
            datetime: { type: "string", format: "date-time", description: "Datetime of the price, provided by the price feed" },
            diff_sec: { type: "number", description: "Difference between the requested `utc_datetime` and price `datetime`" },
            price: { type: "string", example: "12.345678" },
          },
          required: ["datetime", "diff_sec", "price"],
        },
      },
    },
    required: ["priceFeed", "priceRows"],
  };

  async getPricesAroundADate(priceType: PriceType, oracle_id: string, datetime: Date, lookAround: SamplingPeriod, halfLimit: number) {
    const isoDatetime = datetime.toISOString();
    const priceFeed = await db_query_one<{ price_feed_id: number; feed_key: string }>(
      `
      select 
        price_feed_id, feed_key 
      from price_feed 
      where price_feed_data->>'externalId' = %L 
        and (
          case 
            when %L = 'share_to_underlying' 
              then feed_key like 'beefy:%:ppfs' 
            when %L = 'underlying_to_usd'
              then feed_key like 'beefy-data:%' 
            else false
          end
        )
      order by price_feed_id asc
      limit 1
    `,
      [oracle_id, priceType, priceType],
      this.services.db,
    );
    if (!priceFeed) {
      return null;
    }
    const queryParams = [isoDatetime, priceFeed.price_feed_id, isoDatetime, lookAround, isoDatetime, lookAround, isoDatetime, halfLimit];

    const priceRows = await db_query<{
      datetime: Date;
      diff_sec: number;
      price: string;
    }>(
      `
      SELECT
        *
      FROM (
        (
          SELECT 
            datetime, 
            EXTRACT(EPOCH FROM (datetime - %L::timestamptz))::integer as diff_sec,
            price
          FROM price_ts
          WHERE price_feed_id = %L
            and datetime between %L::timestamptz - %L::interval and %L::timestamptz + %L::interval 
            and datetime <= %L 
          ORDER BY datetime DESC 
          LIMIT %L
        ) 
          UNION ALL
        (
          SELECT 
            datetime, 
            EXTRACT(EPOCH FROM (datetime - %L::timestamptz))::integer as diff_sec,
            price
          FROM price_ts
          WHERE price_feed_id = %L
            and datetime between %L::timestamptz - %L::interval and %L::timestamptz + %L::interval 
            and datetime > %L 
          ORDER BY datetime ASC 
          LIMIT %L
        )
      ) as t
      ORDER BY abs(diff_sec) ASC
      `,
      [...queryParams, ...queryParams],
      this.services.db,
    );

    return { priceFeed, priceRows };
  }
}
