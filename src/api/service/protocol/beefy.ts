import { Chain } from "../../../types/chain";
import { DbClient, db_query } from "../../../utils/db";
import { productKeyExamples } from "../../schema/product";
import { AsyncCache } from "../cache";
import { PriceService } from "../price";
import { ProductService } from "../product";

export class BeefyPortfolioService {
  constructor(private services: { db: DbClient; cache: AsyncCache; product: ProductService; price: PriceService }) {}

  public static timelineSchemaComponents = {
    InvestorTimelineRow: {
      $id: "InvestorTimelineRow",
      type: "object",
      properties: {
        datetime: { type: "string", format: "date-time", description: "The transaction datetime" },
        product_key: { type: "string", description: "The product key", example: productKeyExamples[0] },
        display_name: { type: "string", description: "The product display name", example: "synapse-syn-weth" },
        chain: { $ref: "ChainEnum" },
        is_eol: { type: "boolean", description: "Whether the product is EOL" },
        is_dashboard_eol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
        transaction_hash: {
          type: "string",
          nullable: true,
          description: "The transaction hash",
          example: "0x63437809ee6419b892ad12a3b2b9ac7f72c151600c0c65484c9fa2113f4bfb54",
        },
        share_to_underlying_price: { type: "number", description: "The share to underlying price" },
        underlying_to_usd_price: { type: "number", nullable: true, description: "The underlying to USD price" },
        share_balance: { type: "number", description: "The share balance" },
        underlying_balance: { type: "number", description: "The underlying balance" },
        usd_balance: { type: "number", nullable: true, description: "The USD balance" },
        share_diff: { type: "number", description: "The share diff" },
        underlying_diff: { type: "number", description: "The underlying diff" },
        usd_diff: { type: "number", nullable: true, description: "The USD diff" },
      },
      required: [
        "datetime",
        "product_key",
        "display_name",
        "chain",
        "is_eol",
        "is_dashboard_eol",
        "transaction_hash",
        "share_to_underlying_price",
        "underlying_to_usd_price",
        "share_balance",
        "underlying_balance",
        "usd_balance",
        "share_diff",
        "underlying_diff",
        "usd_diff",
      ],
    },
  };

  public static investorTimelineSchema = {
    description: "The investor timeline, list of all deposit and withdraw transaction for a given investor",
    type: "array",
    items: { $ref: "InvestorTimelineRow" },
  };

  async getInvestorTimeline(investorId: number) {
    const cacheKey = `api:portfolio-service:timeline:${investorId}}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      return db_query<{
        datetime: string;
        product_key: string;
        display_name: string;
        chain: Chain;
        is_eol: boolean;
        is_dashboard_eol: boolean;
        transaction_hash: string | null;
        share_to_underlying_price: number;
        underlying_to_usd_price: number | null;
        share_balance: number;
        underlying_balance: number;
        usd_balance: number | null;
        share_diff: number;
        underlying_diff: number;
        usd_diff: number | null;
      }>(
        `
            select b.datetime,
              p.product_key,
              coalesce(p.product_data->'vault'->>'id', p.product_data->'boost'->>'id')::text as display_name,
              p.chain,
              coalesce(p.product_data->'vault'->>'eol', p.product_data->'boost'->>'eol')::text = 'true' as is_eol,
              (p.product_data->>'dashboardEol')::text = 'true' as is_dashboard_eol,
              bytea_to_hexstr(b.transaction_hash) as transaction_hash,
              b.share_to_underlying_price, 
              b.underlying_to_usd_price,
              b.balance as share_balance, 
              b.underlying_balance,
              b.usd_balance,
              b.balance_diff as share_diff, 
              b.underlying_diff,
              b.usd_diff
            from beefy_investor_timeline_cache_ts b
            join product p on p.product_id = b.product_id
            where b.investor_id = %L
            order by product_key asc, datetime asc
        `,
        [investorId],
        this.services.db,
      );
    });
  }

  public static investorCountsSchemaComponents = {
    InvestorCountResult: {
      $id: "InvestorCountResult",
      type: "object",
      properties: {
        investor_count_columns: {
          type: "array",
          items: { type: "string" },
          description: "The columns for the investor counts as the actual data is contained in an array to reduce the size of the response",
          example: ["investor_count", "investor_count_more_than_1_usd", "investor_count_more_than_10_usd"],
        },
        items: {
          type: "array",
          items: { $ref: "InvestorCountItem" },
          description: "The actual investor counts for each product",
        },
      },
      required: ["investor_count_columns", "items"],
    },

    InvestorCountItem: {
      $id: "InvestorCountItem",
      type: "object",
      properties: {
        product_key: { type: "string", description: "The product key", example: productKeyExamples[0] },
        beefy_id: { type: "string", description: "The beefy vault id", example: "cake-bnb" },
        investor_counts: {
          type: "array",
          items: { type: "number" },
          description:
            "Investor counts, each value corresponds to the column names in investor_count_columns. The first value is the total investor count, the second value is the investor count with a balance > 1 USD, the third value is the investor count with a balance > 10 USD, etc.",
          example: [123, 12, 1],
        },
      },
      required: ["product_key", "beefy_id", "investor_counts"],
    },
  };

  public static investorCountsSchema = {
    description:
      "Count for all beefy products, including the number of investors with a balance > 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000",
    $ref: "InvestorCountResult",
  };

  async getInvestorCounts() {
    const cacheKey = `api:portfolio-service:investor-count`;
    const ttl = 1000 * 60 * 60 * 24; // 1 day cache
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const results = await db_query<{
        product_key: string;
        beefy_id: string;
        investor_count: number;
        investor_count_more_than_1_usd: number;
        investor_count_more_than_10_usd: number;
        investor_count_more_than_100_usd: number;
        investor_count_more_than_1000_usd: number;
        investor_count_more_than_10000_usd: number;
        investor_count_more_than_100000_usd: number;
        investor_count_more_than_1000000_usd: number;
        investor_count_more_than_10000000_usd: number;
      }>(
        `
          with last_balance as (
            select
              product_id,
              investor_id,
              last(usd_balance, datetime) as last_usd_balance
            from beefy_investor_timeline_cache_ts
            where product_id IN ( 
                select product_id 
                from product 
                where coalesce(product_data->>'dashboardEol')::text = 'false'
            )
            group by 1,2
          ), 
          investor_count_by_product_id as (
            select 
              product_id, 
              count(*) as investor_count,
              count(*) filter(where last_usd_balance > 1) as investor_count_more_than_1_usd,
              count(*) filter(where last_usd_balance > 10) as investor_count_more_than_10_usd,
              count(*) filter(where last_usd_balance > 100) as investor_count_more_than_100_usd,
              count(*) filter(where last_usd_balance > 1000) as investor_count_more_than_1000_usd,
              count(*) filter(where last_usd_balance > 10000) as investor_count_more_than_10000_usd,
              count(*) filter(where last_usd_balance > 100000) as investor_count_more_than_100000_usd,
              count(*) filter(where last_usd_balance > 1000000) as investor_count_more_than_1000000_usd,
              count(*) filter(where last_usd_balance > 10000000) as investor_count_more_than_10000000_usd
            from last_balance
            group by 1
          )
          select 
            p.product_key, 
            coalesce(p.product_data->'vault'->>'id', p.product_data->'boost'->>'id') as beefy_id, 
            c.investor_count,
            c.investor_count_more_than_1_usd,
            c.investor_count_more_than_10_usd,
            c.investor_count_more_than_100_usd,
            c.investor_count_more_than_1000_usd,
            c.investor_count_more_than_10000_usd,
            c.investor_count_more_than_100000_usd,
            c.investor_count_more_than_1000000_usd,
            c.investor_count_more_than_10000000_usd
          from investor_count_by_product_id c
          join product p using(product_id)
        `,
        [],
        this.services.db,
      );

      return {
        investor_count_columns: [
          "investor_count",
          "investor_count_more_than_1_usd",
          "investor_count_more_than_10_usd",
          "investor_count_more_than_100_usd",
          "investor_count_more_than_1000_usd",
          "investor_count_more_than_10000_usd",
          "investor_count_more_than_100000_usd",
          "investor_count_more_than_1000000_usd",
          "investor_count_more_than_10000000_usd",
        ],
        items: results.map((row) => ({
          product_key: row.product_key,
          beefy_id: row.beefy_id,
          investor_counts: [
            row.investor_count,
            row.investor_count_more_than_1_usd,
            row.investor_count_more_than_10_usd,
            row.investor_count_more_than_100_usd,
            row.investor_count_more_than_1000_usd,
            row.investor_count_more_than_10000_usd,
            row.investor_count_more_than_100000_usd,
            row.investor_count_more_than_1000000_usd,
            row.investor_count_more_than_10000000_usd,
          ],
        })),
      };
    });
  }
}
