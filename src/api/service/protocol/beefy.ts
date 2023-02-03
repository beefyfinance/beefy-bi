import Decimal from "decimal.js";
import { Chain } from "../../../types/chain";
import { SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import { DbClient, db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { TimeBucket, timeBucketToSamplingPeriod } from "../../schema/time-bucket";
import { AsyncCache } from "../cache";
import { PriceService } from "../price";
import { ProductService } from "../product";

const logger = rootLogger.child({ module: "api", component: "portfolio-service" });

export class BeefyPortfolioService {
  constructor(private services: { db: DbClient; cache: AsyncCache; product: ProductService; price: PriceService }) {}

  async getInvestorPortfolioValue(investorId: number) {
    const cacheKey = `api:portfolio-service:current-value:${investorId}}`;
    const ttl = 1000 * 60 * 5; // 5 min
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const rawLastBalance = await db_query<{
        product_id: number;
        price_feed_1_id: number;
        price_feed_2_id: number;
        pending_rewards_price_feed_id: number;
        product_key: string;
        display_name: string;
        chain: Chain;
        is_eol: boolean;
        datetime: Date;
        balance: string;
        pending_rewards: string;
      }>(
        `
          select 
            p.product_id,
            p.price_feed_1_id,
            p.price_feed_2_id,
            p.pending_rewards_price_feed_id,
            p.product_key,
            coalesce(p.product_data->'vault'->>'id', p.product_data->'boost'->>'id')::text as display_name,
            p.chain,
            coalesce(p.product_data->'vault'->>'eol', p.product_data->'boost'->>'eol')::text = 'true' as is_eol,
            last(datetime, datetime) as datetime,
            last(balance, datetime) as balance,
            last(pending_rewards, datetime) as pending_rewards
          from beefy_investor_timeline_cache_ts b
            join product p on b.product_id = p.product_id
          where b.investor_id = %L
          group by 1,2,3,4,5,6,7,8
        `,
        [investorId],
        this.services.db,
      );

      const lastPriceMap = await this.services.price.getLastPrices(
        [
          ...rawLastBalance.map((x) => x.price_feed_1_id),
          ...rawLastBalance.map((x) => x.price_feed_2_id),
          ...rawLastBalance.map((x) => x.pending_rewards_price_feed_id),
        ].filter((x) => x), // remove nulls
      );

      const lastBalance = rawLastBalance
        .map((x) => ({
          ...x,
          balance: x.balance ? new Decimal(x.balance) : null,
          share_to_underlying_price: lastPriceMap.get(x.price_feed_1_id) ?? null,
          underlying_to_usd_price: lastPriceMap.get(x.price_feed_2_id) ?? null,
          pending_rewards: x.pending_rewards ? new Decimal(x.pending_rewards) : null,
          pending_rewards_usd_price: lastPriceMap.get(x.pending_rewards_price_feed_id) ?? null,
        }))
        .map((x) => ({
          ...x,
          share_balance: x.balance?.mul(x.share_to_underlying_price ?? 0) ?? null,
          pending_rewards_usd_balance: x.pending_rewards?.mul(x.pending_rewards_usd_price ?? 0) ?? null,
        }))
        .map((x) => ({
          ...x,
          underlying_balance: x.share_balance?.mul(x.underlying_to_usd_price ?? 0) ?? null,
        }))
        .map((x) => ({
          ...x,
          usd_balance: x.underlying_balance?.mul(x.underlying_to_usd_price ?? 0) ?? null,
        }));

      logger.trace({ msg: "getInvestorPortfolioValue", data: { investorId, lastBalance } });
      return lastBalance;
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
