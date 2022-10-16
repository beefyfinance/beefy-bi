import Decimal from "decimal.js";
import { groupBy, keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { SupportedRangeTypes } from "../../../utils/range";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "prices" });

export interface DbPrice {
  datetime: Date;
  priceFeedId: number;
  blockNumber: number;
  price: Decimal;
  priceData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertPrice$<
  TTarget,
  TRange extends SupportedRangeTypes,
  TParams extends DbPrice,
  TInput extends ImportQuery<TTarget, TRange>,
  TRes extends ImportQuery<TTarget, TRange>,
>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter<TTarget, TRange>;
  getPriceData: (obj: TInput) => TParams;
  formatOutput: (obj: TInput, price: DbPrice) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return dbBatchCall$({
    client: options.client,
    streamConfig: options.streamConfig,
    emitErrors: options.emitErrors,
    formatOutput: options.formatOutput,
    getData: options.getPriceData,
    processBatch: async (objAndData) => {
      // add duplicate detection in dev only
      if (process.env.NODE_ENV === "development") {
        const duplicates = Object.entries(groupBy(objAndData, ({ data }) => `${data.priceFeedId}-${data.blockNumber}`)).filter(
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
          uniqBy(objAndData, ({ data }) => `${data.priceFeedId}-${data.blockNumber}`).map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.priceFeedId,
            data.price.toString(),
            data.priceData,
          ]),
        ],
        options.client,
      );
      return objAndData.map(({ obj, data: investment }) => investment);
    },
  });
}
