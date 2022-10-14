import Decimal from "decimal.js";
import { groupBy, keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "prices" });

export interface DbPrice {
  datetime: Date;
  priceFeedId: number;
  blockNumber: number;
  price: Decimal;
  priceData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertPrice$<TInput, TRes>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  getPriceData: (obj: TInput) => DbPrice;
  formatOutput: (obj: TInput, price: DbPrice) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // insert to the price table
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, price: options.getPriceData(obj) }));

      // add duplicate detection in dev only
      if (process.env.NODE_ENV === "development") {
        const duplicates = Object.entries(groupBy(objAndData, ({ price }) => `${price.priceFeedId}-${price.blockNumber}`)).filter(
          ([_, v]) => v.length > 1,
        );
        if (duplicates.length > 0) {
          logger.error({ msg: "Duplicate prices", data: duplicates });
        }
      }

      await db_query(
        `INSERT INTO price_ts (
              datetime,
              block_number,
              price_feed_id,
              price,
              price_data
          ) VALUES %L
              ON CONFLICT (price_feed_id, block_number, datetime) 
              DO UPDATE SET 
                price = EXCLUDED.price, 
                price_data = jsonb_merge(price_ts.price_data, EXCLUDED.price_data)
          `,
        [
          uniqBy(objAndData, ({ price }) => `${price.priceFeedId}-${price.blockNumber}`).map(({ price }) => [
            price.datetime.toISOString(),
            price.blockNumber,
            price.priceFeedId,
            price.price.toString(),
            price.priceData,
          ]),
        ],
        options.client,
      );
      return objAndData.map(({ obj, price: investment }) => options.formatOutput(obj, investment));
    }, options.streamConfig.workConcurrency),

    Rx.concatMap((investments) => investments), // flatten
  );
}
