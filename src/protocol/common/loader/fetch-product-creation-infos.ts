import { keyBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";
import { DbImportState } from "./import-state";
import { DbProduct } from "./product";

export function fetchPriceFeedContractCreationInfos<TObj, TCtx extends ImportCtx<TObj>, TRes>(options: {
  ctx: TCtx;
  getPriceFeedId: (obj: TObj) => number;
  which: "price-feed-1" | "price-feed-2";
  importStateType: DbImportState["importData"]["type"];
  productType: DbProduct["productData"]["type"];
  formatOutput: (
    obj: TObj,
    contractCreationInfos: { chain: Chain; productId: number; contractCreatedAtBlock: number; contractCreationDate: Date } | null,
  ) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return dbBatchCall$({
    ctx: options.ctx,
    getData: options.getPriceFeedId,
    formatOutput: options.formatOutput,
    logInfos: { msg: "fetchPriceFeedContractCreationInfos", data: { which: options.which } },
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
            JOIN product p on p.product_id = (i.import_data->'productId')::integer and i.import_data->>'type' = %L
          WHERE ${fieldName} IN (%L)
            and p.product_data->>'type' = %L`,
        [options.importStateType, objAndData.map((obj) => obj.data), options.productType],
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
      return new Map(objAndData.map(({ data }) => [data, idMap[data] ?? null]));
    },
  });
}
