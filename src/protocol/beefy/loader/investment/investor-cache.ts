import Decimal from "decimal.js";
import { keyBy, uniqBy } from "lodash";
import { Nullable } from "../../../../types/ts";
import { DbClient, db_query } from "../../../../utils/db";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { dbBatchCall$ } from "../../../common/utils/db-batch";

const logger = rootLogger.child({ module: "common", component: "investment" });

interface DbInvestorCacheDimensions {
  investorId: number;
  productId: number;
  datetime: Date;
  blockNumber: number;
}

type DbInvestorCacheChainInfos = Nullable<{
  balance: Decimal; // moo balance
  balanceDiff: Decimal; // balance - previous balance
  shareToUnderlyingPrice: Decimal; // ppfs
  underlyingBalance: Decimal; // balance * shareToUnderlyingPrice
  underlyingDiff: Decimal; // balanceDiff * shareToUnderlyingPrice
  pendingRewards: Decimal; // pending rewards
  pendingRewardsDiff: Decimal; // pendingRewards - previous pendingRewards
}>;

// usd price infos are added afterwards
type DbInvestorCacheUsdInfos = Nullable<{
  pendingRewardsToUsdPrice: Decimal; // token price
  pendingRewardsUsdBalance: Decimal; // pendingRewards * pendingRewardsToUsdPrice
  pendingRewardsUsdDiff: Decimal; // pendingRewardsDiff * pendingRewardsToUsdPrice
  underlyingToUsdPrice: Decimal; // lp price
  usdBalance: Decimal; // underlyingBalance * underlyingToUsdPrice
  usdDiff: Decimal; // underlyingDiff * underlyingToUsdPrice
}>;

export type DbInvestorCache = DbInvestorCacheDimensions & DbInvestorCacheChainInfos & DbInvestorCacheUsdInfos;

export function upsertInvestorCacheChainInfos$<
  TObj,
  TErr extends ErrorEmitter<TObj>,
  TRes,
  TParams extends DbInvestorCacheDimensions & DbInvestorCacheChainInfos,
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getInvestorCacheChainInfos: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbInvestorCacheChainInfos) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getInvestorCacheChainInfos,
    logInfos: { msg: "upsertInvestorCache" },
    processBatch: async (objAndData) => {
      // data should come from investment_balance_ts upsert so it should be ok and not need further processing

      const result = await db_query<{ product_id: number; investor_id: number; block_number: number }>(
        `INSERT INTO beefy_investor_timeline_cache_ts (
            investor_id,
            product_id,
            datetime,
            block_number,
            balance,
            balance_diff,
            share_to_underlying_price,
            underlying_balance,
            underlying_diff,
            pending_rewards,
            pending_rewards_diff
          ) VALUES %L
            ON CONFLICT (product_id, investor_id, block_number, datetime) 
            DO UPDATE SET 
                balance = coalesce(EXCLUDED.balance, beefy_investor_timeline_cache_ts.balance),
                balance_diff = coalesce(EXCLUDED.balance_diff, beefy_investor_timeline_cache_ts.balance_diff),
                share_to_underlying_price = coalesce(EXCLUDED.share_to_underlying_price, beefy_investor_timeline_cache_ts.share_to_underlying_price),
                underlying_to_usd_price = coalesce(EXCLUDED.underlying_to_usd_price, beefy_investor_timeline_cache_ts.underlying_to_usd_price),
                underlying_balance = coalesce(EXCLUDED.underlying_balance, beefy_investor_timeline_cache_ts.underlying_balance),
                underlying_diff = coalesce(EXCLUDED.underlying_diff, beefy_investor_timeline_cache_ts.underlying_diff),
                pending_rewards = coalesce(EXCLUDED.pending_rewards, beefy_investor_timeline_cache_ts.pending_rewards),
                pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, beefy_investor_timeline_cache_ts.pending_rewards_diff)
            RETURNING product_id, investor_id, block_number
          `,
        [
          uniqBy(objAndData, ({ data }) => `${data.productId}:${data.investorId}:${data.blockNumber}`).map(({ data }) => [
            data.investorId,
            data.productId,
            data.datetime.toISOString(),
            data.blockNumber,
            data.balance ? data.balance.toString() : null,
            data.balanceDiff ? data.balanceDiff.toString() : null,
            data.shareToUnderlyingPrice ? data.shareToUnderlyingPrice.toString() : null,
            data.underlyingBalance ? data.underlyingBalance.toString() : null,
            data.underlyingDiff ? data.underlyingDiff.toString() : null,
            data.pendingRewards ? data.pendingRewards.toString() : null,
            data.pendingRewardsDiff ? data.pendingRewardsDiff.toString() : null,
          ]),
        ],
        options.ctx.client,
      );

      // update debug data
      const idMap = keyBy(result, (result) => `${result.product_id}:${result.investor_id}:${result.block_number}`);
      return new Map(
        objAndData.map(({ data }) => {
          const key = `${data.productId}:${data.investorId}:${data.blockNumber}`;
          const result = idMap[key];
          if (!result) {
            throw new ProgrammerError({ msg: "Upserted investment cache not found", data });
          }
          return [data, data];
        }),
      );
    },
  });
}

export async function addMissingInvestorCacheUsdInfos(options: { client: DbClient }) {
  await db_query<{ product_id: number; investor_id: number; block_number: number }>(
    `
      insert into beefy_investor_timeline_cache_ts (
        investor_id,
        product_id,
        datetime,
        block_number,
        balance,
        balance_diff,
        share_to_underlying_price,
        underlying_balance,
        underlying_diff,
        underlying_to_usd_price,
        usd_balance,
        usd_diff,
        pending_rewards,
        pending_rewards_diff
      ) (
        with balance_scope as (
          select *
          from beefy_investor_timeline_cache_ts
          where underlying_to_usd_price is null
        ),
        investment_diff_raw as (
          select b.datetime, b.block_number, b.investor_id, b.product_id,
            last(b.balance, b.datetime) as balance,
            sum(b.balance_diff) as balance_diff,
            last(pr1.price::numeric, pr1.datetime) as price1,
            last(pr2.price::numeric, pr2.datetime) as price2,
            last(b.pending_rewards, b.datetime) as pending_reward,
            sum(b.pending_rewards_diff) as pending_reward_diff
          from balance_scope b
          join product p
            on b.product_id = p.product_id
          -- we should have the exact price1 (share to underlying) from this exact block for all investment change
          join price_ts pr1
            on p.price_feed_1_id = pr1.price_feed_id
            and pr1.datetime = b.datetime
            and pr1.block_number = b.block_number
          -- but for price 2 (underlying to usd) we need to match on approx time
          join price_ts pr2
            on p.price_feed_2_id = pr2.price_feed_id
            and time_bucket('15min', pr2.datetime) = time_bucket('15min', b.datetime)
          where b.balance_diff != 0 -- only show changes, not reward snapshots
          group by 1,2,3,4
          having sum(b.balance_diff) != 0 -- only show changes, not reward snapshots
        )
        select
          b.investor_id,
          b.product_id,
          b.datetime,
          b.block_number,
          b.balance as balance,
          b.balance_diff as balance_diff,
          b.price1 as share_to_underlying_price,
          (b.balance * b.price1)::NUMERIC(100, 24) as underlying_balance,
          (b.balance_diff * b.price1)::NUMERIC(100, 24) as underlying_diff,
          b.price2 as underlying_to_usd_price,
          (b.balance * b.price1 * b.price2)::NUMERIC(100, 24) as usd_balance,
          (b.balance_diff * b.price1 * b.price2)::NUMERIC(100, 24) as usd_diff,
          b.pending_reward as pending_reward,
          b.pending_reward_diff as pending_reward_diff
        from investment_diff_raw b
        join product p on p.product_id = b.product_id
      )
      ON CONFLICT (product_id, investor_id, block_number, datetime)
      DO UPDATE SET
          balance = coalesce(EXCLUDED.balance, beefy_investor_timeline_cache_ts.balance),
          balance_diff = coalesce(EXCLUDED.balance_diff, beefy_investor_timeline_cache_ts.balance_diff),
          share_to_underlying_price = coalesce(EXCLUDED.share_to_underlying_price, beefy_investor_timeline_cache_ts.share_to_underlying_price),
          underlying_to_usd_price = coalesce(EXCLUDED.underlying_to_usd_price, beefy_investor_timeline_cache_ts.underlying_to_usd_price),
          underlying_balance = coalesce(EXCLUDED.underlying_balance, beefy_investor_timeline_cache_ts.underlying_balance),
          underlying_diff = coalesce(EXCLUDED.underlying_diff, beefy_investor_timeline_cache_ts.underlying_diff),
          pending_rewards = coalesce(EXCLUDED.pending_rewards, beefy_investor_timeline_cache_ts.pending_rewards),
          pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, beefy_investor_timeline_cache_ts.pending_rewards_diff);

      `,
    [],
    options.client,
  );
}
