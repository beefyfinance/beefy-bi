import { groupBy, keyBy, uniq } from "lodash";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { DbClient, db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { BeefyBoost } from "../../beefy/connector/boost-list";
import { BeefyVault } from "../../beefy/connector/vault-list";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
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
  pendingRewardsPriceFeedId: null;
  productData: {
    type: "beefy:vault";
    dashboardEol: boolean;
    vault: BeefyVault;
  };
}
export interface DbBeefyGovVaultProduct extends DbBaseProduct {
  priceFeedId1: number; // no ppfs for gov vaults, but we added one for consistency
  priceFeedId2: number; // underlying price
  pendingRewardsPriceFeedId: number; // in gas token
  productData: {
    type: "beefy:gov-vault";
    dashboardEol: boolean;
    vault: BeefyVault;
  };
}
export interface DbBeefyBoostProduct extends DbBaseProduct {
  priceFeedId1: number; // ppfs of vault
  priceFeedId2: number; // underlying price of vault
  pendingRewardsPriceFeedId: number; // in whatever token the boost is for
  productData: {
    type: "beefy:boost";
    dashboardEol: boolean;
    boost: BeefyBoost;
  };
}
export type DbBeefyProduct = DbBeefyStdVaultProduct | DbBeefyGovVaultProduct | DbBeefyBoostProduct;

export type DbProduct = DbBeefyProduct;

export function upsertProduct$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends Omit<DbProduct, "productId">>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getProductData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbProduct) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getProductData,
    logInfos: { msg: "upsert product" },
    processBatch: async (objAndData) => {
      // select existing products to update them instead of inserting new ones so we don't burn too much serial ids
      // More details: https://dba.stackexchange.com/a/295356/65636
      const existingProducts = await db_query<DbProduct>(
        `SELECT 
          product_id as "productId",
          chain,
          product_key as "productKey",
          price_feed_1_id as "priceFeedId1",
          price_feed_2_id as "priceFeedId2",
          pending_rewards_price_feed_id as "pendingRewardsPriceFeedId",
          product_data as "productData"
        FROM product
        where product_key in (%L)`,
        [objAndData.map(({ data }) => data.productKey)],
        options.ctx.client,
      );

      const existingProductByProductKey = keyBy(existingProducts, "productKey");
      const productsToInsert = objAndData.filter(({ data }) => !existingProductByProductKey[data.productKey]);
      const productsToUpdate = objAndData.filter(({ data }) => existingProductByProductKey[data.productKey]);

      const insertResults =
        productsToInsert.length <= 0
          ? []
          : await db_query<DbProduct>(
              `INSERT INTO product (product_key, price_feed_1_id, price_feed_2_id, pending_rewards_price_feed_id, chain, product_data) VALUES %L
              ON CONFLICT (product_key) 
              DO UPDATE SET
                chain = EXCLUDED.chain,
                price_feed_1_id = EXCLUDED.price_feed_1_id,
                price_feed_2_id = EXCLUDED.price_feed_2_id,
                pending_rewards_price_feed_id = EXCLUDED.pending_rewards_price_feed_id,
                product_data = EXCLUDED.product_data
              RETURNING 
                product_id as "productId",
                chain,
                product_key as "productKey",
                price_feed_1_id as "priceFeedId1",
                price_feed_2_id as "priceFeedId2",
                pending_rewards_price_feed_id as "pendingRewardsPriceFeedId",
                product_data as "productData"`,
              [
                productsToInsert.map(({ data }) => [
                  data.productKey,
                  data.priceFeedId1,
                  data.priceFeedId2,
                  data.pendingRewardsPriceFeedId,
                  data.chain,
                  data.productData,
                ]),
              ],
              options.ctx.client,
            );

      const updateResults =
        productsToUpdate.length <= 0
          ? []
          : await db_query<DbProduct>(
              `UPDATE product SET
                chain = u.chain::chain_enum,
                price_feed_1_id = u.price_feed_1_id::integer,
                price_feed_2_id = u.price_feed_2_id::integer,
                pending_rewards_price_feed_id = u.pending_rewards_price_feed_id::integer,
                product_data = u.product_data
              FROM (VALUES %L) AS u(product_key, chain, price_feed_1_id, price_feed_2_id, pending_rewards_price_feed_id, product_data)
              WHERE product.product_key = u.product_key
              RETURNING 
                product.product_id as "productId",
                product.chain,
                product.product_key as "productKey",
                product.price_feed_1_id as "priceFeedId1",
                product.price_feed_2_id as "priceFeedId2",
                product.pending_rewards_price_feed_id as "pendingRewardsPriceFeedId",
                product.product_data as "productData"`,
              [
                productsToUpdate.map(({ data }) => [
                  data.productKey,
                  data.chain,
                  data.priceFeedId1,
                  data.priceFeedId2,
                  data.pendingRewardsPriceFeedId,
                  data.productData,
                ]),
              ],
              options.ctx.client,
            );

      const results = [...insertResults, ...updateResults];
      // return a map where keys are the original parameters object refs
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

export function fetchProduct$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends number>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getProductId: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, product: DbProduct) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getProductId,
    logInfos: { msg: "fetch product" },
    processBatch: async (objAndData) => {
      const results = await db_query<DbProduct>(
        `SELECT
            product_id as "productId", 
            product_key as "productKey", 
            price_feed_1_id as "priceFeedId1", 
            price_feed_2_id as "priceFeedId2", 
            pending_rewards_price_feed_id as "pendingRewardsPriceFeedId",
            chain, 
            product_data as "productData"
        FROM product 
        WHERE product_id IN (%L)`,
        [uniq(objAndData.map(({ data }) => data))],
        options.ctx.client,
      );

      // return a map where keys are the original parameters object refs
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

export function productList$<TKey extends string>(client: DbClient, keyPrefix: TKey, chain: Chain | null): Rx.Observable<DbProduct> {
  logger.debug({ msg: "Fetching vaults from db" });
  return Rx.of(
    db_query<DbProduct>(
      `SELECT 
        product_id as "productId",
        chain,
        product_key as "productKey",
        price_feed_1_id as "priceFeedId1",
        price_feed_2_id as "priceFeedId2",
        pending_rewards_price_feed_id as "pendingRewardsPriceFeedId",
        product_data as "productData"
      FROM product
      where product_key like %L || ':%'
        and chain in (%L)`,
      [keyPrefix, chain === null ? allChainIds : [chain]],
      client,
    ),
  ).pipe(
    Rx.mergeAll(),

    Rx.tap((products) => logger.debug({ msg: "emitting product list", data: { count: products.length, chain, keyPrefix } })),

    Rx.concatMap((products) => Rx.from(products)), // flatten
  );
}

export function chainProductIds$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends Chain>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getChain: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, productIds: number[]) => TRes;
}) {
  const operator$ = dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: (item, productIds: number[]) => ({ input: item, output: productIds }),
    getData: options.getChain,
    logInfos: { msg: "fetch chain products" },
    processBatch: async (objAndData) => {
      const results = await db_query<{ chain: Chain; product_id: number }>(
        `select chain, product_id
        from product
        where chain in (%L)`,
        [uniq(objAndData.map(({ data }) => data))],
        options.ctx.client,
      );

      // return a map where keys are the original parameters object refs
      const idMap = groupBy(results, "chain");

      return new Map(
        objAndData.map(({ data }) => {
          const res = idMap[data];
          if (!res) {
            throw new Error("Could not find products for this chain");
          }
          return [data, res.map((r) => r.product_id)];
        }),
      );
    },
  });

  return cacheOperatorResult$({
    operator$,
    cacheConfig: {
      type: "global",
      globalKey: "chain-product-ids",
      stdTTLSec: 30 * 60 /* 30 min */,
      useClones: false,
    },
    getCacheKey: (item) => `chain-products-${options.getChain(item)}`,
    logInfos: { msg: "product id list for chain", data: {} },
    formatOutput: (item, productIds: number[]) => options.formatOutput(item, productIds),
  });
}
