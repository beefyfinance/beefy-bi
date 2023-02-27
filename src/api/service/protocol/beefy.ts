import { Chain } from "../../../types/chain";
import { DbClient, db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { AsyncCache } from "../cache";
import { PriceService } from "../price";
import { ProductService } from "../product";

const logger = rootLogger.child({ module: "api", component: "portfolio-service" });

export class BeefyPortfolioService {
  constructor(private services: { db: DbClient; cache: AsyncCache; product: ProductService; price: PriceService }) {}

  async getInvestorTimeline(investorId: number) {
    const cacheKey = `api:portfolio-service:timeline:${investorId}}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      return db_query<{
        datetime: Date;
        product_key: string;
        display_name: string;
        chain: Chain;
        is_eol: boolean;
        is_dashboard_eol: boolean;
        transaction_hash: string;
        share_to_underlying_price: string;
        underlying_to_usd_price: string;
        share_balance: string;
        underlying_balance: string;
        usd_balance: string;
        share_diff: string;
        underlying_diff: string;
        usd_diff: string;
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
              and coalesce(p.product_data->>'dashboardEol')::text = 'false'
            order by product_key asc, datetime asc
        `,
        [investorId],
        this.services.db,
      );
    });
  }
}
