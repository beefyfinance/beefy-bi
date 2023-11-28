import { keyBy } from "lodash";
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
        investor_count_historical_columns: {
          type: "array",
          items: { type: "string" },
          description:
            "The columns for the historical investor counts as the actual data is contained in an array to reduce the size of the response",
          example: ["hist_3d", "hist_7d", "hist_15d", "hist_30d"],
        },
        historical_datetimes: {
          type: "array",
          items: { type: "string", format: "date-time" },
          description: "The datetime for the historical investor counts",
          example: ["2021-08-01T00:00:00.000Z", "2021-08-02T00:00:00.000Z", "2021-08-03T00:00:00.000Z", "2021-08-04T00:00:00.000Z"],
        },
        items: {
          type: "array",
          items: { $ref: "InvestorCountItem" },
          description: "The actual investor counts for each product",
        },
      },
      required: ["investor_count_columns", "investor_count_historical_columns", "historical_datetimes", "items"],
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
        historical_counts: {
          type: "array",
          items: { type: ["number", "null"] },
          description:
            "Historical investor counts, each value corresponds to the column names in investor_count_historical_columns. The first value is the investor count diff 3 days ago, the second value is the investor count diff 7 days ago, the third value is the investor count diff 15 days ago, etc.",
          example: [123, 12, 1],
        },
      },
      required: ["product_key", "beefy_id", "investor_counts", "historical_counts"],
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

    const countCols = [
      "investor_count",
      "investor_count_more_than_1_usd",
      "investor_count_more_than_10_usd",
      "investor_count_more_than_100_usd",
      "investor_count_more_than_1000_usd",
      "investor_count_more_than_10000_usd",
      "investor_count_more_than_100000_usd",
      "investor_count_more_than_1000000_usd",
      "investor_count_more_than_10000000_usd",
    ];

    const liveProducts = await this.services.product.getProducts(false);
    const liveProductIds = liveProducts.map((p) => p.productId);
    if (liveProductIds.length <= 0) {
      return {
        investor_count_columns: countCols,
        items: [],
      };
    }

    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const latestCountResult = await db_query<{
        product_id: number;
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
            where product_id IN (%L)
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
            p.product_id,
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
        [liveProductIds],
        this.services.db,
      );

      const historicalCountsResult = await db_query<{
        product_id: number;
        hist_3d_datetime: string;
        hist_3d_approx_investor_count: number;
        hist_7d_datetime: string;
        hist_7d_approx_investor_count: number;
        hist_15d_datetime: string;
        hist_15d_approx_investor_count: number;
        hist_30d_datetime: string;
        hist_30d_approx_investor_count: number;
      }>(
        `
          SELECT
            s.product_id,
            last(s.datetime, s.datetime) filter (where datetime < now() - interval '3 day') AS "hist_3d_datetime",
            last(s.datetime, s.datetime) filter (where datetime < now() - interval '7 days') AS "hist_7d_datetime",
            last(s.datetime, s.datetime) filter (where datetime < now() - interval '15 days') AS "hist_15d_datetime",
            last(s.datetime, s.datetime) filter (where datetime < now() - interval '30 days') AS "hist_30d_datetime",
            (last(distinct_count(s.approx_count_investor_hll_all), s.datetime) filter (where datetime < now() - interval '3 day'))::integer as "hist_3d_approx_investor_count",
            (last(distinct_count(s.approx_count_investor_hll_all), s.datetime) filter (where datetime < now() - interval '7 day'))::integer as "hist_7d_approx_investor_count",
            (last(distinct_count(s.approx_count_investor_hll_all), s.datetime) filter (where datetime < now() - interval '15 day'))::integer as "hist_15d_approx_investor_count",
            (last(distinct_count(s.approx_count_investor_hll_all), s.datetime) filter (where datetime < now() - interval '30 day'))::integer as "hist_30d_approx_investor_count"
          FROM product_stats_investor_counts_with_segments_1d_ts s
          WHERE
            datetime < now() - interval '1 day'
            and datetime >= now() - interval '31 day'
            and product_id in (%L)
          group by 1
        `,
        [liveProductIds],
        this.services.db,
      );

      const historicalCountsResultByProductId = keyBy(historicalCountsResult, "product_id");
      const histDatetimes: Array<Date | null> = [null, null, null, null];
      const productCounts = latestCountResult.map((row) => {
        const histDiff = historicalCountsResultByProductId[row.product_id] || null;
        if (histDiff) {
          // it's ugly but it's fast
          if (histDatetimes[0] === null) {
            histDatetimes[0] = new Date(histDiff.hist_3d_datetime);
          }
          if (histDatetimes[1] === null) {
            histDatetimes[1] = new Date(histDiff.hist_7d_datetime);
          }
          if (histDatetimes[2] === null) {
            histDatetimes[2] = new Date(histDiff.hist_15d_datetime);
          }
          if (histDatetimes[3] === null) {
            histDatetimes[3] = new Date(histDiff.hist_30d_datetime);
          }
        }
        return {
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
          historical_counts: histDiff
            ? [
                histDiff.hist_3d_approx_investor_count,
                histDiff.hist_7d_approx_investor_count,
                histDiff.hist_15d_approx_investor_count,
                histDiff.hist_30d_approx_investor_count,
              ]
            : [null, null, null, null],
        };
      });

      return {
        investor_count_columns: countCols,
        investor_count_historical_columns: ["hist_3d", "hist_7d", "hist_15d", "hist_30d"],
        historical_datetimes: histDatetimes,
        items: productCounts,
      };
    });
  }
}
