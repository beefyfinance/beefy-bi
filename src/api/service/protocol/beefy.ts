import { Chain } from "../../../types/chain";
import { SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import { DbClient, db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { TimeBucket, timeBucketToSamplingPeriod } from "../../schema/time-bucket";
import { AsyncCache } from "../cache";
import { ProductService } from "../product";

const logger = rootLogger.child({ module: "api", component: "portfolio-service" });

export class BeefyPortfolioService {
  constructor(private services: { db: DbClient; cache: AsyncCache; product: ProductService }) {}

  async getInvestorPortfolioValue(investorId: number) {
    const cacheKey = `api:portfolio-service:current-value:${investorId}}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const res = await db_query<{
        product_id: number;
        product_key: string;
        display_name: string;
        chain: Chain;
        is_eol: boolean;
        share_to_underlying_price: string;
        underlying_to_usd_price: string;
        share_balance: string;
        underlying_balance: string;
        usd_balance: string;
        pending_rewards: string;
        pending_rewards_usd_balance: string;
      }>(
        `
          select 
            p.product_id,
            p.product_key,
            coalesce(p.product_data->'vault'->>'id', p.product_data->'boost'->>'id')::text as display_name,
            p.chain,
            coalesce(p.product_data->'vault'->>'eol', p.product_data->'boost'->>'eol')::text = 'true' as is_eol,
            b.share_to_underlying_price, 
            b.underlying_to_usd_price,
            b.balance as share_balance, 
            b.underlying_balance,
            b.usd_balance,
            b.pending_rewards,
            b.pending_rewards_usd_balance
          from beefy_investor_timeline_cache_ts b
            join product p on b.product_id = p.product_id
          where b.investor_id = %L
          order by 1
        `,
        [investorId],
        this.services.db,
      );

      logger.trace({ msg: "getInvestorPortfolioValue", data: { investorId, res } });
      return res;
    });
  }

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
