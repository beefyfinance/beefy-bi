import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { BeefyVault } from "../../beefy/connector/vault-list";
import { BeefyBoost } from "../../beefy/connector/boost-list";

const logger = rootLogger.child({ module: "product" });

interface DbBaseProduct {
  productId: number;
  productKey: string;
  chain: Chain;
  priceFeedId: number;
}

export interface DbBeefyVaultProduct extends DbBaseProduct {
  productData: {
    type: "beefy:vault";
    vault: BeefyVault;
  };
}
export interface DbBeefyBoostProduct extends DbBaseProduct {
  productData: {
    type: "beefy:boost";
    boost: BeefyBoost;
  };
}
export type DbBeefyProduct = DbBeefyVaultProduct | DbBeefyBoostProduct;

export type DbProduct = DbBeefyProduct;

export function upsertProduct$<TInput, TRes>(options: {
  client: PoolClient;
  getProductData: (obj: TInput) => Omit<DbProduct, "productId">;
  formatOutput: (obj: TInput, feed: DbProduct) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    // batch queries
    Rx.bufferCount(500),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, productData: options.getProductData(obj) }));

      const results = await db_query<DbProduct>(
        `INSERT INTO product (product_key, price_feed_id, chain, product_data) VALUES %L
              ON CONFLICT (product_key) 
              DO UPDATE SET product_data = jsonb_merge(product.product_data, EXCLUDED.product_data)
              RETURNING product_id as "productId", product_key as "productKey", price_feed_id as "priceFeedId", chain, product_data as "productData"`,
        [
          objAndData.map(({ productData }) => [
            productData.productKey,
            productData.priceFeedId,
            productData.chain,
            productData.productData,
          ]),
        ],
        options.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "productKey");

      return objAndData.map((obj) => options.formatOutput(obj.obj, idMap[obj.productData.productKey]));
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function productList$<TKey extends string>(client: PoolClient, keyPrefix: TKey): Rx.Observable<DbProduct> {
  logger.debug({ msg: "Fetching vaults from db" });
  return Rx.of(
    db_query<DbProduct>(
      `SELECT 
        product_id as "productId",
        chain,
        price_feed_id as "priceFeedId",
        product_data as "productData"
      FROM product
      where product_key like %L || ':%'`,
      [keyPrefix],
      client,
    ).then((products) => (products.length > 0 ? products : Promise.reject("No product found"))),
  ).pipe(
    Rx.mergeAll(),

    Rx.tap((products) => logger.debug({ msg: "emitting product list", data: { count: products.length } })),

    Rx.concatMap((products) => Rx.from(products)), // flatten
  );
}
