import Decimal from "decimal.js";
import { groupBy } from "lodash";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "common", component: "investment" });

interface DbInvestment {
  datetime: Date;
  blockNumber: number;
  productId: number;
  investorId: number;
  balance: Decimal;
  balanceDiff: Decimal;
  pendingRewards: Decimal | null;
  pendingRewardsDiff: Decimal | null;
  transactionHash: string;
}

export function upsertInvestment$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbInvestment>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getInvestmentData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbInvestment) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getInvestmentData,
    logInfos: { msg: "upsertInvestment" },
    processBatch: async (objAndData) => {
      // @todo: this is a temporary fix to avoid duplicate investments, we shouldn't have duplicates in the first place
      // merge investments before inserting
      const groups = groupBy(objAndData, (objAndData) => `${objAndData.data.productId}:${objAndData.data.investorId}:${objAndData.data.blockNumber}`);
      const investments: typeof objAndData = [];
      for (const group of Object.values(groups)) {
        if (group.length === 1) {
          investments.push(group[0]);
          continue;
        }

        // if all balances are equal, just merge them
        const balance = group[0].data.balance;
        const balanceDiff = group[0].data.balanceDiff;
        const pendingRewards = group[0].data.pendingRewards;
        const pendingRewardsDiff = group[0].data.pendingRewardsDiff;
        const isAllInvestmentTheSame = group.every(
          (objAndData) =>
            objAndData.data.balance.eq(balance) &&
            objAndData.data.balanceDiff.eq(balanceDiff) &&
            (pendingRewards
              ? objAndData.data.pendingRewards && objAndData.data.pendingRewards.eq(pendingRewards)
              : !objAndData.data.pendingRewards) &&
            (pendingRewardsDiff
              ? objAndData.data.pendingRewardsDiff && objAndData.data.pendingRewardsDiff.eq(pendingRewardsDiff)
              : !objAndData.data.pendingRewardsDiff),
        );
        if (isAllInvestmentTheSame) {
          logger.debug({ msg: "upsertInvestment: all investments are the same, merging them", data: group });
          investments.push({
            obj: group[0].obj,
            data: {
              ...group[0].data,
            },
          });
        } else {
          logger.error({ msg: "upsertInvestment: all investments are not the same, not merging them", data: group });
          throw new ProgrammerError("upsertInvestment: all investments are not the same, cannot merge them");
        }
      }

      await db_query<{
        datetime: Date;
        product_id: number;
        investor_id: number;
        block_number: number;
        balance: string;
        balance_diff: string;
        pending_rewards: string;
        pending_rewards_diff: string;
      }>(
        `INSERT INTO investment_balance_ts (
            datetime,
            block_number,
            product_id,
            investor_id,
            balance,
            balance_diff,
            pending_rewards,
            pending_rewards_diff,
            transaction_hash
        ) VALUES %L
            ON CONFLICT (product_id, investor_id, block_number, datetime) 
            DO UPDATE SET 
              balance = EXCLUDED.balance, 
              balance_diff = EXCLUDED.balance_diff,
              pending_rewards = coalesce(investment_balance_ts.pending_rewards, EXCLUDED.pending_rewards),
              pending_rewards_diff = coalesce(investment_balance_ts.pending_rewards_diff, EXCLUDED.pending_rewards_diff),
              transaction_hash = EXCLUDED.transaction_hash
          RETURNING datetime, product_id, investor_id, block_number, balance, balance_diff, pending_rewards, pending_rewards_diff
        `,
        [
          investments.map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.productId,
            data.investorId,
            data.balance.toString(),
            data.balanceDiff.toString(),
            data.pendingRewards?.toString() || null,
            data.pendingRewardsDiff?.toString() || null,
            strAddressToPgBytea(data.transactionHash),
          ]),
        ],
        options.ctx.client,
      );

      // update debug data
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}
