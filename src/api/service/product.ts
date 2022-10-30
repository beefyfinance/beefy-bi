import { PoolClient } from "pg";
import { DbProduct } from "../../protocol/common/loader/product";
import { db_query_one } from "../../utils/db";
import { AsyncCache } from "./cache";

export class ProductService {
  constructor(private services: { db: PoolClient; cache: AsyncCache }) {}

  async getSingleProductPriceFeedIds(productId: number) {
    const cacheKey = `api:product-service:price-feeds:${productId}`;
    const ttl = 1000 * 60 * 60 * 24 * 7; // 1 week
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<{
        price_feed_1_id: number;
        price_feed_2_id: number;
      }>(` SELECT price_feed_1_id, price_feed_2_id FROM product where product_id = %L `, [productId], this.services.db),
    );
  }

  async getPriceFeedIds(productIds: number[]) {
    return (await Promise.all(productIds.map((productId) => this.getSingleProductPriceFeedIds(productId)))).filter(
      (pfs): pfs is NonNullable<typeof pfs> => pfs !== null,
    );
  }

  async getProductByProductKey(productKey: string) {
    const cacheKey = `api:product-service:product:${productKey}`;
    const ttl = 1000 * 60 * 60 * 24 * 7; // 1 week
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<DbProduct>(
        `SELECT 
          product_id as "productId", 
          product_key as "productKey", 
          price_feed_1_id as "priceFeedId1", 
          price_feed_2_id as "priceFeedId2", 
          chain, 
          product_data as "productData" 
        FROM product 
        where product_key = %L`,
        [productKey],
        this.services.db,
      ),
    );
  }
}
