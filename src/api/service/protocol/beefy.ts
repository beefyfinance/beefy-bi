import { Chain } from "../../../types/chain";
import { DbClient, db_query } from "../../../utils/db";
import { productKeyExamples } from "../../schema/product";
import { AsyncCache } from "../cache";
import { PriceService } from "../price";
import { ProductService } from "../product";

export class BeefyPortfolioService {
  constructor(private services: { db: DbClient; cache: AsyncCache; product: ProductService; price: PriceService }) {}

  public static schemaComponents = {
    InvestorTimelineRow: {
      $id: "InvestorTimelineRow",
      type: "object",
      properties: {
        datetime: { type: "string", format: "date-time", description: "The transaction datetime" },
        product_key: { type: "string", description: "The product key", example: productKeyExamples },
        display_name: { type: "string", description: "The product display name" },
        chain: { $ref: "ChainEnum" },
        is_eol: { type: "boolean", description: "Whether the product is EOL" },
        is_dashboard_eol: { type: "boolean", description: "Whether the product is EOL on the dashboard" },
        transaction_hash: { type: "string", nullable: true, description: "The transaction hash" },
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
}
