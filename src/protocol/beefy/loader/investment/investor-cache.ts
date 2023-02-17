import Decimal from "decimal.js";
import { keyBy, uniqBy } from "lodash";
import { Nullable } from "../../../../types/ts";
import { DbClient, db_query, strAddressToPgBytea } from "../../../../utils/db";
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
  transactionHash: string;
  // denormalized fiels
  priceFeed1Id: number;
  priceFeed2Id: number;
  pendingRewardsPriceFeedId: number | null;
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
            transaction_hash,
            price_feed_1_id,
            price_feed_2_id,
            pending_rewards_price_feed_id,
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
                transaction_hash = coalesce(EXCLUDED.transaction_hash, beefy_investor_timeline_cache_ts.transaction_hash),
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
            strAddressToPgBytea(data.transactionHash),
            data.priceFeed1Id,
            data.priceFeed2Id,
            data.pendingRewardsPriceFeedId,
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
      with missing_cache_prices as materialized (
        select
          c.investor_id,
          c.product_id,
          c.datetime,
          c.block_number,
          last(pr2.price, pr2.datetime) as price
        from beefy_investor_timeline_cache_ts c
          join price_ts pr2 on c.price_feed_2_id = pr2.price_feed_id
          and time_bucket('15min', pr2.datetime) = time_bucket('15min', c.datetime)
        where c.underlying_to_usd_price is null
        group by 1,2,3,4
      )
      update beefy_investor_timeline_cache_ts
      set
        underlying_to_usd_price = to_update.price,
        usd_balance = underlying_balance * to_update.price,
        usd_diff = underlying_diff * to_update.price
      from missing_cache_prices to_update
      where to_update.investor_id = beefy_investor_timeline_cache_ts.investor_id
        and to_update.product_id = beefy_investor_timeline_cache_ts.product_id
        and to_update.datetime = beefy_investor_timeline_cache_ts.datetime
        and to_update.block_number = beefy_investor_timeline_cache_ts.block_number;
      `,
    [],
    options.client,
  );
}
