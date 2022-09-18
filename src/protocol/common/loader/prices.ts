import Decimal from "decimal.js";
import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger2";

const logger = rootLogger.child({ module: "prices" });

export function upsertPrices<
  TObj extends {
    datetime: Date;
    assetPriceFeedId: number;
    usdValue: Decimal;
  },
>(client: PoolClient): Rx.OperatorFunction<TObj, TObj> {
  return Rx.pipe(
    // batch by some reasonable amount for the database
    Rx.bufferCount(5000),

    // insert into the prices table
    Rx.mergeMap(async (prices) => {
      // short circuit if there's nothing to do
      if (prices.length === 0) {
        return [];
      }

      logger.debug({ msg: "inserting prices", data: { count: prices.length } });

      await db_query<{}>(
        `INSERT INTO asset_price_ts (
            datetime,
            asset_price_feed_id,
            usd_value
          ) VALUES %L
          ON CONFLICT (asset_price_feed_id, datetime) DO NOTHING`,
        [prices.map((price) => [price.datetime, price.assetPriceFeedId, price.usdValue.toString()])],
        client,
      );
      return prices;
    }),

    Rx.mergeMap((prices) => Rx.from(prices)),
  );
}

export function toMissingPriceDataQuery<TObj extends { assetPriceFeedId: number }>(
  client: PoolClient,
): Rx.OperatorFunction<TObj, { obj: TObj; fromDate: Date; toDate: Date }> {
  return Rx.pipe(
    // work by batches to be kind on the database
    Rx.bufferCount(200),

    // find out which data is missing
    Rx.mergeMap(async (objs) => {
      const results = await db_query<{ price_feed_key: string; last_inserted_datetime: Date }>(
        `SELECT 
            asset_price_feed_id,
            last(datetime, datetime) as last_inserted_datetime
          FROM asset_price_ts 
          WHERE asset_price_feed_id IN (%L)
          GROUP BY asset_price_feed_id`,
        [objs.map((o) => o.assetPriceFeedId)],
        client,
      );
      const resultsMap = keyBy(results, "asset_price_feed_id");

      // if we have data already, we want to only fetch new data
      // otherwise, we aim for the last 24h of data
      let fromDate = new Date(new Date().getTime() - 1000 * 60 * 60 * 24);
      let toDate = new Date();
      return objs.map((o) => {
        if (resultsMap[o.assetPriceFeedId]?.last_inserted_datetime) {
          fromDate = resultsMap[o.assetPriceFeedId].last_inserted_datetime;
        }
        return {
          obj: o,
          fromDate,
          toDate,
        };
      });
    }),

    // ok, flatten all price feed queries
    Rx.mergeMap((priceFeedQueries) => Rx.from(priceFeedQueries)),
  );
}
