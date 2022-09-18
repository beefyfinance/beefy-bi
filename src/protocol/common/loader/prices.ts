import Decimal from "decimal.js";
import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "prices" });

interface DbPrice {
  datetime: Date;
  priceFeedId: number;
  usdValue: Decimal;
}

export function upsertPrices<TInput, TRes>(options: {
  client: PoolClient;
  getPriceData: (obj: TInput) => DbPrice;
  formatOutput: (obj: TInput, price: DbPrice) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    // batch by some reasonable amount for the database
    Rx.bufferCount(5000),

    // insert into the prices table
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, priceData: options.getPriceData(obj) }));

      logger.debug({ msg: "inserting prices", data: { count: objAndData.length } });

      await db_query<{}>(
        `INSERT INTO asset_price_ts (
            datetime,
            price_feed_id,
            usd_value
          ) VALUES %L
          ON CONFLICT (price_feed_id, datetime) DO NOTHING`,
        [objAndData.map(({ priceData }) => [priceData.datetime, priceData.priceFeedId, priceData.usdValue.toString()])],
        options.client,
      );

      return objAndData.map((obj) => options.formatOutput(obj.obj, obj.priceData));
    }),

    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}

export function findMissingPriceRangeInDb<TObj, TRes>(options: {
  client: PoolClient;
  getFeedId: (obj: TObj) => number;
  formatOutput: (obj: TObj, missingData: { fromDate: Date; toDate: Date }) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // work by batches to be kind on the database
    Rx.bufferCount(500),

    // find out which data is missing
    Rx.mergeMap(async (objs) => {
      const results = await db_query<{ priceFeedId: number; lastInsertedDatetime: Date }>(
        `SELECT 
            price_feed_id as "priceFeedId",
            last(datetime, datetime) as "lastInsertedDatetime"
          FROM asset_price_ts 
          WHERE price_feed_id IN (%L)
          GROUP BY price_feed_id`,
        [objs.map((o) => options.getFeedId(o))],
        options.client,
      );
      const resultsMap = keyBy(results, "priceFeedId");

      // if we have data already, we want to only fetch new data
      // otherwise, we aim for the last 24h of data
      let fromDate = new Date(new Date().getTime() - 1000 * 60 * 60 * 24);
      let toDate = new Date();
      return objs.map((o) => {
        if (resultsMap[options.getFeedId(o)]?.lastInsertedDatetime) {
          fromDate = resultsMap[options.getFeedId(o)].lastInsertedDatetime;
        }
        return options.formatOutput(o, { fromDate, toDate });
      });
    }),

    // ok, flatten all price feed queries
    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}
