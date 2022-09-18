import { keyBy, omit } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger2";
import { BeefyVault } from "../../../types/beefy";

const logger = rootLogger.child({ module: "product" });

interface DbBaseProduct {
  productId: number;
  productKey: string;
  chain: Chain;
  assetPriceFeedId: number;
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
    boost: { id: string };
  };
}
export type DbBeefyProduct = DbBeefyVaultProduct | DbBeefyBoostProduct;

export type DbProduct = DbBeefyProduct;

export function upsertProduct<
  TObj extends {
    product: {
      productKey: string;
      chain: Chain;
      assetPriceFeedId: number;
      productData: object;
    };
  },
>(client: PoolClient): Rx.OperatorFunction<TObj, Omit<TObj, "product"> & { productId: number }> {
  return Rx.pipe(
    // batch queries
    Rx.bufferCount(500),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      type TRes = { product_id: number; product_key: string };
      const results = await db_query<TRes>(
        `INSERT INTO product (product_key, asset_price_feed_id, chain, product_data) VALUES %L
              ON CONFLICT (product_key) 
              DO UPDATE SET product_data = jsonb_merge(product.product_data, EXCLUDED.product_data)
              RETURNING product_id, product_key`,
        [objs.map(({ product }) => [product.productKey, product.assetPriceFeedId, product.chain, product.productData])],
        client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "product_key");
      return objs.map((obj) => {
        return omit({ ...obj, productId: idMap[obj.product.productKey].product_id }, "product");
      });
    }),

    // flatten objects
    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}

export function productList$<TKey extends string>(client: PoolClient, keyPrefix: TKey): Rx.Observable<DbProduct> {
  logger.debug({ msg: "Fetching vaults from db" });
  return Rx.of(
    db_query<DbProduct>(
      `SELECT 
        product_id as "productId",
        chain,
        asset_price_feed_id as "assetPriceFeedId",
        product_data as "productData"
      FROM product
      where product_key like %L || ':%'`,
      [keyPrefix],
      client,
    ).then((products) => (products.length > 0 ? products : Promise.reject("No product found"))),
  ).pipe(
    Rx.mergeAll(),

    Rx.tap((products) => logger.debug({ msg: "emitting product list", data: { count: products.length } })),

    Rx.mergeMap((products) => Rx.from(products)), // flatten
  );
}
