import { DbProduct } from "../../protocol/common/loader/product";
import { Chain, allChainIds } from "../../types/chain";
import { DbClient, db_query, db_query_one } from "../../utils/db";
import { AsyncCache } from "./cache";

const productSchema = {
  type: "object",
  properties: {
    productKey: { type: "string", description: "Functional product identifier" },
    chain: { type: "string", enum: allChainIds, description: "The chain identifier" },
    productData: {
      anyOf: [
        {
          type: "object",
          description: "The vault data",
          properties: {
            type: { type: "string", enum: ["beefy:vault", "beefy:gov-vault"], description: "The product type" },
            dashboardEol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
            vault: {
              type: "object",
              properties: {
                id: { type: "string", description: "The vault id" },
                chain: { type: "string", enum: allChainIds, description: "The chain identifier" },
                token_name: { type: "string", description: "The token name" },
                token_decimals: { type: "number", description: "The token decimals" },
                contract_address: { type: "string", description: "The vault contract address" },
                want_address: { type: "string", description: "The want token address" },
                want_decimals: { type: "number", description: "The want token decimals" },
                eol: { type: "boolean", description: "Whether the vault is EOL" },
                eol_date: { type: "string", nullable: true, format: "date-time", description: "The EOL date" },
                assets: {
                  type: "array",
                  items: { type: "string" },
                },
                protocol: { type: "string", description: "The protocol identifier" },
                protocol_product: { type: "string", description: "The protocol product identifier" },
                want_price_feed_key: { type: "string", description: "The want price feed key" },
                is_gov_vault: { type: "boolean", description: "Whether the vault is a governance vault" },
                gov_vault_reward_token_symbol: {
                  type: "string",
                  nullable: true,
                  description: "The governance vault reward token symbol",
                },
                gov_vault_reward_token_address: {
                  type: "string",
                  nullable: true,
                  description: "The governance vault reward token address",
                },
                gov_vault_reward_token_decimals: {
                  type: "number",
                  nullable: true,
                  description: "The governance vault reward token decimals",
                },
              },
              required: [
                "id",
                "chain",
                "token_name",
                "token_decimals",
                "contract_address",
                "want_address",
                "want_decimals",
                "eol",
                "eol_date",
                "assets",
                "protocol",
                "protocol_product",
                "want_price_feed_key",
                "is_gov_vault",
              ],
            },
          },
          required: ["type", "dashboardEol", "vault"],
        },
        {
          type: "object",
          description: "The boost data",
          properties: {
            type: { type: "string", enum: ["beefy:boost"], description: "The product type" },
            dashboardEol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
            boost: {
              type: "object",
              properties: {
                id: { type: "string", description: "The boost id" },
                chain: { type: "string", enum: allChainIds, description: "The chain identifier" },
                vault_id: { type: "string", description: "The vault id" },
                name: { type: "string", description: "The boost name" },
                contract_address: { type: "string", description: "The boost contract address" },
                eol: { type: "boolean", description: "Whether the boost is EOL" },
                eol_date: { type: "string", nullable: true, format: "date-time", description: "The EOL date" },
                staked_token_address: { type: "string", description: "The staked token address" },
                staked_token_decimals: { type: "number", description: "The staked token decimals" },
                vault_want_address: { type: "string", description: "The vault want address" },
                vault_want_decimals: { type: "number", description: "The vault want decimals" },
                reward_token_decimals: { type: "number", description: "The reward token decimals" },
                reward_token_symbol: { type: "string", description: "The reward token symbol" },
                reward_token_address: { type: "string", description: "The reward token address" },
                reward_token_price_feed_key: { type: "string", description: "The reward token price feed key" },
              },
              required: [
                "id",
                "chain",
                "vault_id",
                "name",
                "eol",
                "eol_date",
                "contract_address",
                "staked_token_address",
                "staked_token_decimals",
                "vault_want_address",
                "vault_want_decimals",
                "reward_token_decimals",
                "reward_token_symbol",
                "reward_token_address",
                "reward_token_price_feed_key",
              ],
            },
          },
          required: ["type", "dashboardEol", "boost"],
        },
      ],
    },
  },
  required: ["productKey", "chain", "productData"],
};

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

  public static allProductsSchema = {
    description: "List of all beefy products, vaults, boosts, governance vaults, etc.",
    type: "array",
    items: productSchema,
  };

  async getAllProducts(includeEol = false, chain: Chain) {
    const cacheKey = `api:product-service:all-products:${chain}:include-eol-${includeEol}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<DbProduct>(
        `SELECT 
          product_key as "productKey",
          chain, 
          product_data as "productData" 
        FROM product
        WHERE chain = %L
        AND (product_data->>'dashboardEol') in (%L)`,
        [chain, includeEol ? ["true", "false"] : ["false"]],
        this.services.db,
      ),
    );
  }

  public static oneProductSchema = productSchema;

  async getProductByChainAndContractAddress(chain: Chain, contractAddress: string) {
    contractAddress = contractAddress.toLowerCase();
    const cacheKey = `api:product-service:product:${chain}:${contractAddress}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<DbProduct>(
        `SELECT 
          product_key as "productKey",
          chain, 
          product_data as "productData" 
        FROM product
        WHERE chain = %L
        AND lower(coalesce(
          product_data->'vault'->>'contract_address', 
          product_data->'boost'->>'contract_address'
        )) = %L`,
        [chain, contractAddress],
        this.services.db,
      ),
    );
  }
}
