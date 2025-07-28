import { DbProduct, appProductTypes } from "../../protocol/common/loader/product";
import { Chain } from "../../types/chain";
import { DbClient, db_query, db_query_one } from "../../utils/db";
import { productKeyExamples } from "../schema/product";
import { AsyncCache } from "./cache";

export class ProductService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  public static schemaComponents = {
    ProductTypeEnum: {
      $id: "ProductTypeEnum",
      type: "string",
      enum: appProductTypes,
      description: "The product type",
    },
    ProductStdVault: {
      $id: "ProductStdVault",
      type: "object",
      description: "A vault definition",
      properties: {
        type: { $ref: "ProductTypeEnum", description: "The product type" },
        dashboardEol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
        vault: {
          type: "object",
          properties: {
            id: { type: "string", description: "The vault id", example: "convex-steth" },
            chain: { $ref: "ChainEnum" },
            token_name: { type: "string", description: "The token name", example: "mooConvexStETH" },
            token_decimals: { type: "number", description: "The token decimals", example: 18 },
            contract_address: { type: "string", description: "The vault contract address", example: "0xa7739fd3d12ac7F16D8329AF3Ee407e19De10D8D" },
            want_address: { type: "string", description: "The want token address", example: "0x06325440D014e39736583c165C2963BA99fAf14E" },
            want_decimals: { type: "number", description: "The want token decimals", example: 18 },
            eol: { type: "boolean", description: "Whether the vault is EOL" },
            eol_date: { type: "string", nullable: true, format: "date-time", description: "The EOL date" },
            assets: { type: "array", items: { type: "string" }, example: ["stETH", "ETH"] },
            protocol: { type: "string", description: "The protocol identifier", example: "convex" },
            protocol_product: { type: "string", description: "The protocol product identifier", example: "steth" },
            want_price_feed_key: { type: "string", description: "The want token external price feed id", example: "convex-steth" },
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
          ],
        },
      },
      required: ["type", "dashboardEol", "vault"],
    },
    ProductBridgedVault: {
      $id: "ProductBridgedVault",
      type: "object",
      description: "A bridged vault definition",
      properties: {
        type: { $ref: "ProductTypeEnum", description: "The product type" },
        dashboardEol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
        vault: {
          type: "object",
          properties: {
            id: { type: "string", description: "The vault id", example: "convex-steth" },
            chain: { $ref: "ChainEnum" },
            token_name: { type: "string", description: "The token name", example: "mooConvexStETH" },
            token_decimals: { type: "number", description: "The token decimals", example: 18 },
            contract_address: { type: "string", description: "The vault contract address", example: "0xa7739fd3d12ac7F16D8329AF3Ee407e19De10D8D" },
            want_address: { type: "string", description: "The want token address", example: "0x06325440D014e39736583c165C2963BA99fAf14E" },
            want_decimals: { type: "number", description: "The want token decimals", example: 18 },
            eol: { type: "boolean", description: "Whether the vault is EOL" },
            eol_date: { type: "string", nullable: true, format: "date-time", description: "The EOL date" },
            assets: { type: "array", items: { type: "string" }, example: ["stETH", "ETH"] },
            protocol: { type: "string", description: "The protocol identifier", example: "convex" },
            protocol_product: { type: "string", description: "The protocol product identifier", example: "steth" },
            want_price_feed_key: { type: "string", description: "The want token external price feed id", example: "convex-steth" },
            bridged_version_of: {
              $ref: "ProductStdVault",
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
            "bridged_version_of",
          ],
        },
      },
      required: ["type", "dashboardEol", "vault"],
    },
    ProductBoost: {
      $id: "ProductBoost",
      type: "object",
      description: "A boost definition",
      properties: {
        type: { $ref: "ProductTypeEnum", description: "The product type" },
        dashboardEol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
        boost: {
          type: "object",
          properties: {
            id: { type: "string", description: "The boost id", example: "moo_convex-steth-lido" },
            chain: { $ref: "ChainEnum" },
            vault_id: { type: "string", description: "The vault id this boost refers to", example: "convex-steth" },
            name: { type: "string", description: "The boost name", example: "Lido" },
            contract_address: { type: "string", description: "The boost contract address", example: "0xAe3F0C61F3Dc48767ccCeF3aD50b29437BE4b1a4" },
            eol: { type: "boolean", description: "Whether the boost is EOL" },
            eol_date: { type: "string", nullable: true, format: "date-time", description: "The EOL date" },
            staked_token_address: { type: "string", description: "The staked token address", example: "0xa7739fd3d12ac7F16D8329AF3Ee407e19De10D8D" },
            staked_token_decimals: { type: "number", description: "The staked token decimals", example: 18 },
            vault_want_address: { type: "string", description: "The vault want address", example: "0x06325440D014e39736583c165C2963BA99fAf14E" },
            vault_want_decimals: { type: "number", description: "The vault want decimals", example: 18 },
            reward_token_decimals: { type: "number", description: "The reward token decimals", example: 18 },
            reward_token_symbol: { type: "string", description: "The reward token symbol", example: "LDO" },
            reward_token_address: { type: "string", description: "The reward token address", example: "0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32" },
            reward_token_price_feed_key: { type: "string", description: "The reward token external price feed id", example: "LDO" },
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
    ProductGovVault: {
      $id: "ProductGovVault",
      type: "object",
      description: "A governance vault definition",
      properties: {
        type: { $ref: "ProductTypeEnum", description: "The product type" },
        dashboardEol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
        vault: {
          type: "object",
          properties: {
            id: { type: "string", description: "The vault id", example: "ethereum-bifi-gov" },
            chain: { $ref: "ChainEnum" },
            token_name: { type: "string", description: "The earned token name", example: "WETH" },
            token_decimals: { type: "number", description: "The token decimals", example: 18 },
            contract_address: { type: "string", description: "The vault contract address", example: "0xF49c523F08B4e7c8E51a44088ea2a5e6b5f397D9" },
            want_address: { type: "string", description: "The want token address", example: "0x5870700f1272a1AdbB87C3140bD770880a95e55D" },
            want_decimals: { type: "number", description: "The want token decimals", example: 18 },
            eol: { type: "boolean", description: "Whether the vault is EOL" },
            eol_date: { type: "string", nullable: true, format: "date-time", description: "The EOL date" },
            assets: { type: "array", items: { type: "string" }, example: ["BIFI"] },
            protocol: { type: "string", description: "The protocol identifier", example: "beefy" },
            protocol_product: { type: "string", description: "The protocol product identifier", example: "BIFI" },
            want_price_feed_key: { type: "string", description: "The want price feed key", example: "BIFI" },
            is_gov_vault: { type: "boolean", nullable: true, description: "Whether the vault is a governance vault" },
            gov_vault_reward_token_symbol: { type: "string", nullable: true, description: "The governance vault reward token symbol" },
            gov_vault_reward_token_address: { type: "string", nullable: true, description: "The governance vault reward token address" },
            gov_vault_reward_token_decimals: { type: "number", nullable: true, description: "The governance vault reward token decimals" },
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
          ],
        },
      },
      required: ["type", "dashboardEol", "vault"],
    },
    Product: {
      $id: "Product",
      type: "object",
      description: "A product definition",
      properties: {
        productKey: { type: "string", description: "Functional product identifier", example: productKeyExamples[0] },
        chain: { $ref: "ChainEnum" },
        productData: {
          oneOf: [{ $ref: "ProductStdVault" }, { $ref: "ProductBoost" }, { $ref: "ProductGovVault" }, { $ref: "ProductBridgedVault" }],
          discriminator: {
            propertyName: "type",
            mapping: {
              "beefy:vault": "ProductStdVault",
              "beefy:boost": "ProductBoost",
              "beefy:gov-vault": "ProductGovVault",
              "beefy:bridged-vault": "ProductBridgedVault",
            },
          },
        },
      },
      required: ["productKey", "chain", "productData"],
    },
  };

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
    const lowerCaseProductKey = productKey.toLowerCase();
    const cacheKey = `api:product-service:product:${lowerCaseProductKey}`;
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
        [lowerCaseProductKey],
        this.services.db,
      ),
    );
  }

  public static allProductsSchema = {
    description: "List of all beefy products, vaults, boosts, governance vaults, etc.",
    type: "array",
    items: { $ref: "Product" },
  };

  async getProductsForChain(includeEol = false, chain: Chain) {
    const cacheKey = `api:product-service:products-for-chain:${chain}:include-eol-${includeEol}`;
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

  public static oneProductSchema = {
    $ref: "Product",
  };

  async getProductByChainAndContractAddress(chain: Chain, contractAddress: string) {
    contractAddress = contractAddress.toLowerCase();
    const cacheKey = `api:product-service:product:${chain}:${contractAddress}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<DbProduct>(
        `SELECT 
          product_id as "productId",
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

  async getProducts(includeEol = false) {
    const cacheKey = `api:product-service:all-products:include-eol-${includeEol}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<DbProduct>(
        `SELECT 
          product_id as "productId",
          product_key as "productKey",
          chain, 
          product_data as "productData" 
        FROM product
        WHERE (product_data->>'dashboardEol') in (%L)`,
        [includeEol ? ["true", "false"] : ["false"]],
        this.services.db,
      ),
    );
  }

  async getProductsEarningPoints(includeEol = false) {
    const cacheKey = `api:product-service:all-products-earning-points:include-eol-${includeEol}`;
    const ttl = 1000 * 60 * 60 * 24 * 1; // 1 day
    return this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query<DbProduct>(
        `SELECT 
          product_id as "productId",
          product_key as "productKey",
          chain, 
          product_data as "productData" 
        FROM product p
        WHERE (p.product_data->>'dashboardEol') in (%L)
        AND (p.product_data->'vault'->>'earning_eigenlayer_points') = 'true'`,
        [includeEol ? ["true", "false"] : ["false"]],
        this.services.db,
      ),
    );
  }
}
