import Decimal from "decimal.js";
import { keyBy, uniqBy } from "lodash";
import { Nullable } from "../../../../types/ts";
import { db_query } from "../../../../utils/db";
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
  //underlyingBalance: Decimal; // balance * shareToUnderlyingPrice
  //underlyingDiff: Decimal; // balanceDiff * shareToUnderlyingPrice
  pendingRewards: Decimal; // pending rewards
  pendingRewardsDiff: Decimal; // pendingRewards - previous pendingRewards
}>;

// usd price infos are added afterwards
type DbInvestorCacheUsdInfos = Nullable<{
  pendingRewardsToUsdPrice: Decimal; // token price
  // pendingRewardsUsdBalance: Decimal; // pendingRewards * pendingRewardsToUsdPrice
  // pendingRewardsUsdDiff: Decimal; // pendingRewardsDiff * pendingRewardsToUsdPrice
  underlyingToUsdPrice: Decimal; // lp price
  // usdBalance: Decimal; // underlyingBalance * underlyingToUsdPrice
  // usdDiff: Decimal; // underlyingDiff * underlyingToUsdPrice
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
            pending_rewards,
            pending_rewards_diff
          ) VALUES %L
            ON CONFLICT (product_id, investor_id, block_number, datetime) 
            DO UPDATE SET 
                balance = coalesce(EXCLUDED.balance, balance),
                balance_diff = coalesce(EXCLUDED.balance_diff, balance_diff),
                share_to_underlying_price = coalesce(EXCLUDED.share_to_underlying_price, share_to_underlying_price),
                underlying_to_usd_price = coalesce(EXCLUDED.underlying_to_usd_price, underlying_to_usd_price),
                pending_rewards = coalesce(EXCLUDED.pending_rewards, pending_rewards),
                pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, pending_rewards_diff)
          `,
        [
          uniqBy(objAndData, ({ data }) => `${data.productId}:${data.investorId}:${data.blockNumber}`).map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.productId,
            data.investorId,
            data.balance ? data.balance.toString() : null,
            data.balanceDiff ? data.balanceDiff.toString() : null,
            data.shareToUnderlyingPrice ? data.shareToUnderlyingPrice.toString() : null,
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
            throw new ProgrammerError({ msg: "Upserted investment not found", data });
          }
          return [data, data];
        }),
      );
    },
  });
}
