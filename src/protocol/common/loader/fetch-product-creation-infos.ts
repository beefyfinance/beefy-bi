import { keyBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

export function fetchPriceFeedContractCreationInfos<TObj, TCtx extends ImportCtx<TObj>, TRes>(options: {
  ctx: TCtx;
  getPriceFeedId: (obj: TObj) => number;
  which: "price-feed-1" | "price-feed-2";
  formatOutput: (
    obj: TObj,
    contractCreationInfos: { chain: Chain; productId: number; contractCreatedAtBlock: number; contractCreationDate: Date } | null,
  ) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return dbBatchCall$({
    ctx: options.ctx,
    getData: options.getPriceFeedId,
    formatOutput: options.formatOutput,
    processBatch: async (objAndData) => {
      type TRes = { priceFeedId: number; productId: number; chain: Chain; contractCreatedAtBlock: number; contractCreationDate: Date };
      const fieldName = options.which === "price-feed-1" ? "price_feed_1_id" : "price_feed_2_id";
      const results = await db_query<TRes>(
        `SELECT 
              p.${fieldName} as "priceFeedId",
              p.product_id as "productId",
              p.chain as "chain",
              (import_data->'contractCreatedAtBlock')::integer as "contractCreatedAtBlock",
              (import_data->>'contractCreationDate')::timestamptz as "contractCreationDate"
          FROM import_state i
            JOIN product p on p.product_id = (i.import_data->'productId')::integer
          WHERE ${fieldName} IN (%L)`,
        [objAndData.map((obj) => obj.data)],
        options.ctx.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(
        results.map((res) => {
          res.contractCreationDate = new Date(res.contractCreationDate);
          return res;
        }),
        "priceFeedId",
      );
      return objAndData.map((obj) => idMap[obj.data] ?? null);
    },
  });
}
