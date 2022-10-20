import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { BeefyBoost } from "../../beefy/connector/boost-list";
import { BeefyVault } from "../../beefy/connector/vault-list";
import { ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "product" });

interface DbBaseProduct {
  productId: number;
  productKey: string;
  chain: Chain;
}

export interface DbBeefyStdVaultProduct extends DbBaseProduct {
  priceFeedId1: number; // ppfs
  priceFeedId2: number; // underlying price
  productData: {
    type: "beefy:vault";
    vault: BeefyVault;
  };
}
export interface DbBeefyGovVaultProduct extends DbBaseProduct {
  priceFeedId1: number; // no ppfs for gov vaults, but we added one for consistency
  priceFeedId2: number; // underlying price
  productData: {
    type: "beefy:gov-vault";
    vault: BeefyVault;
  };
}
export interface DbBeefyBoostProduct extends DbBaseProduct {
  priceFeedId1: number; // ppfs of vault
  priceFeedId2: number; // underlying price of vault
  productData: {
    type: "beefy:boost";
    boost: BeefyBoost;
  };
}
export type DbBeefyProduct = DbBeefyStdVaultProduct | DbBeefyGovVaultProduct | DbBeefyBoostProduct;

export type DbProduct = DbBeefyProduct;

export function upsertProduct$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends Omit<DbProduct, "productId">>(options: {
  ctx: TCtx;
  getProductData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbProduct) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    formatOutput: options.formatOutput,
    getData: options.getProductData,
    processBatch: async (objAndData) => {
      const results = await db_query<DbProduct>(
        `INSERT INTO product (product_key, price_feed_1_id, price_feed_2_id, chain, product_data) VALUES %L
              ON CONFLICT (product_key) 
              DO UPDATE SET
                chain = EXCLUDED.chain,
                product_key = EXCLUDED.product_key,
                price_feed_1_id = EXCLUDED.price_feed_1_id,
                price_feed_2_id = EXCLUDED.price_feed_2_id,
                product_data = jsonb_merge(product.product_data, EXCLUDED.product_data)
              RETURNING 
                product_id as "productId", 
                product_key as "productKey", 
                price_feed_1_id as "priceFeedId1", 
                price_feed_2_id as "priceFeedId2", 
                chain, 
                product_data as "productData"`,
        [objAndData.map(({ data }) => [data.productKey, data.priceFeedId1, data.priceFeedId2, data.chain, data.productData])],
        options.ctx.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "productKey");

      return new Map(
        objAndData.map(({ data }) => {
          const product = idMap[data.productKey];
          if (!product) {
            throw new Error("Could not find product after upsert");
          }
          return [data, product];
        }),
      );
    },
  });
}

export function fetchProduct$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends number>(options: {
  ctx: TCtx;
  getProductId: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, product: DbProduct) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    formatOutput: options.formatOutput,
    getData: options.getProductId,
    processBatch: async (objAndData) => {
      const results = await db_query<DbProduct>(
        `SELECT
            product_id as "productId", 
            product_key as "productKey", 
            price_feed_1_id as "priceFeedId1", 
            price_feed_2_id as "priceFeedId2", 
            chain, 
            product_data as "productData"
        FROM product 
        WHERE product_id IN (%L)`,
        [objAndData.map(({ data }) => data)],
        options.ctx.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "productId");

      return new Map(
        objAndData.map(({ data }) => {
          const product = idMap[data];
          if (!product) {
            throw new Error("Could not find product");
          }
          return [data, product];
        }),
      );
    },
  });
}

export function productList$<TKey extends string>(client: PoolClient, keyPrefix: TKey, chain: Chain): Rx.Observable<DbProduct> {
  logger.debug({ msg: "Fetching vaults from db" });
  return Rx.of(
    db_query<DbProduct>(
      `SELECT 
        product_id as "productId",
        chain,
        product_key as "productKey",
        price_feed_1_id as "priceFeedId1",
        price_feed_2_id as "priceFeedId2",
        product_data as "productData"
      FROM product
      where product_key like %L || ':%'
        and chain = %L`,
      [keyPrefix, chain],
      client,
    ),
  ).pipe(
    Rx.mergeAll(),

    Rx.tap((products) => logger.debug({ msg: "emitting product list", data: { count: products.length } })),

    Rx.concatMap((products) => Rx.from(products)), // flatten
  );
}
