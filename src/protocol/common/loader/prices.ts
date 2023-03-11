import Decimal from "decimal.js";
import { groupBy, isEmpty, uniqBy } from "lodash";
import { v4 as uuid } from "uuid";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "prices" });

export interface DbPrice {
  datetime: Date;
  priceFeedId: number;
  blockNumber: number;
  price: Decimal;
}

// upsert the address of all objects and return the id in the specified field
export function upsertPrice$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbPrice>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPriceData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, price: DbPrice) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getPriceData,
    logInfos: { msg: "upsert price" },
    processBatch: async (objAndData) => {
      // add duplicate detection in dev only
      if (process.env.NODE_ENV === "development") {
        const duplicates = Object.entries(
          groupBy(objAndData, ({ data }) => `${data.priceFeedId}-${data.blockNumber}-${data.price.toString()}`),
        ).filter(([_, v]) => v.length > 1);
        if (duplicates.length > 0) {
          logger.error({ msg: "Duplicate prices", data: duplicates });
        }
      }

      await db_query(
        `INSERT INTO price_ts (
            datetime,
            block_number,
            price_feed_id,
            price
        ) VALUES %L
            ON CONFLICT (price_feed_id, block_number, datetime) 
            DO UPDATE SET 
              price = EXCLUDED.price, 
        `,
        [
          uniqBy(objAndData, ({ data }) => `${data.priceFeedId}-${data.blockNumber}`).map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.priceFeedId,
            data.price.toString(),
          ]),
        ],
        options.ctx.client,
      );

      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}
