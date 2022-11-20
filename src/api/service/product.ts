import { DbProduct } from "../../protocol/common/loader/product";
import { DbClient, db_query_one } from "../../utils/db";
import { AsyncCache } from "./cache";

export class ProductService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getSingleProductPriceFeedIds(productId: number) {
    const cacheKey = `api:product-service:price-feeds:${productId}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<{
        price_feed_1_id: number;
        price_feed_2_id: number;
        pending_rewards_price_feed_id: number | null;
      }>(
        `SELECT price_feed_1_id, price_feed_2_id, pending_rewards_price_feed_id 
        FROM product where product_id = %L`,
        [productId],
        this.services.db,
      ),
    );
  }

  async getPriceFeedIds(productIds: number[]) {
    return (await Promise.all(productIds.map((productId) => this.getSingleProductPriceFeedIds(productId)))).filter(
      (pfs): pfs is NonNullable<typeof pfs> => pfs !== null,
    );
  }

  async getProductByProductKey(productKey: string) {
    const cacheKey = `api:product-service:product:${productKey}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<DbProduct>(
        `SELECT 
          product_id as "productId", 
          product_key as "productKey", 
          price_feed_1_id as "priceFeedId1", 
          price_feed_2_id as "priceFeedId2", 
          pending_rewards_price_feed_id as "pendingRewardsPriceFeedId",
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
