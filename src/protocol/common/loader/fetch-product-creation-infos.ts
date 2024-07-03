import { keyBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";
import { DbImportState } from "./import-state";
import { DbProduct } from "./product";

/**
 * @deprecated: use fetchProductCreationInfos$ instead
 */
export function fetchPriceFeedContractCreationInfos<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
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
    emitError: options.emitError,
    getData: options.getPriceFeedId,
    formatOutput: options.formatOutput,
    logInfos: { msg: "fetchPriceFeedContractCreationInfos", data: { which: options.which } },
    processBatch: async (objAndData) => {
      type TRes = { priceFeedId: number; productId: number; chain: Chain; contractCreatedAtBlock: number; contractCreationDate: Date };
      const fieldName = options.which === "price-feed-1" ? "price_feed_1_id" : "price_feed_2_id";
      const results = await db_query<TRes>(
        `SELECT 
              p.product_id as "productId",
              p.${fieldName} as "priceFeedId",
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

      // return a map where keys are the original parameters object refs
      const idMap = keyBy(
        results.map((res) => {
          res.contractCreationDate = new Date(res.contractCreationDate);

          // fix for lynex-gamma-usdc-lynx
          // https://github.com/beefyfinance/beefy-operations/issues/575
          if (res.productId === 18463618) {
            res.productId = 18463622;
            res.contractCreatedAtBlock = 2493809;
            res.contractCreationDate = new Date(1708833155 * 1000);
          }
          return res;
        }),
        "priceFeedId",
      );
      return new Map(objAndData.map(({ data }) => [data, idMap[data] ?? null]));
    },
  });
}

export function fetchProductCreationInfos$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getProductId: (obj: TObj) => number;
  formatOutput: (obj: TObj, contractCreationInfos: { contractCreatedAtBlock: number; contractCreationDate: Date } | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    getData: options.getProductId,
    formatOutput: options.formatOutput,
    logInfos: { msg: "fetchProductCreationInfos" },
    processBatch: async (objAndData) => {
      type TRes = { productId: number; contractCreatedAtBlock: number; contractCreationDate: Date };
      const results = await db_query<TRes>(
        `SELECT 
              p.product_id as "productId",
              (import_data->'contractCreatedAtBlock')::integer as "contractCreatedAtBlock",
              (import_data->>'contractCreationDate')::timestamptz as "contractCreationDate"
          FROM import_state i
            JOIN product p on p.product_id = (i.import_data->'productId')::integer and i.import_data->>'type' = 'product:investment'
          WHERE p.product_id IN (%L)`,
        [objAndData.map((obj) => obj.data)],
        options.ctx.client,
      );

      // return a map where keys are the original parameters object refs
      const idMap = keyBy(
        results.map((res) => {
          res.contractCreationDate = new Date(res.contractCreationDate);
          return res;
        }),
        "productId",
      );
      return new Map(objAndData.map(({ data }) => [data, idMap[data] ?? null]));
    },
  });
}
