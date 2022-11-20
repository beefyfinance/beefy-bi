import Decimal from "decimal.js";
import { groupBy } from "lodash";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "common", component: "investment" });

export interface DbInvestment {
  datetime: Date;
  blockNumber: number;
  productId: number;
  investorId: number;
  balance: Decimal;
  balanceDiff: Decimal;
  pendingRewards: Decimal | null;
  pendingRewardsDiff: Decimal | null;
  investmentData: object;
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
      await db_query(
        `INSERT INTO investment_balance_ts (
              datetime,
              block_number,
              product_id,
              investor_id,
              balance,
              balance_diff,
              pending_rewards,
              pending_rewards_diff,
              investment_data
          ) VALUES %L
              ON CONFLICT (product_id, investor_id, block_number, datetime) 
              DO UPDATE SET 
                balance = EXCLUDED.balance, 
                balance_diff = EXCLUDED.balance_diff,
                pending_rewards = coalesce(investment_balance_ts.pending_rewards, EXCLUDED.pending_rewards),
                pending_rewards_diff = coalesce(investment_balance_ts.pending_rewards_diff, EXCLUDED.pending_rewards_diff),
                investment_data = jsonb_merge(investment_balance_ts.investment_data, EXCLUDED.investment_data)
          `,
        [
          objAndData.map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.productId,
            data.investorId,
            data.balance.toString(),
            data.balanceDiff.toString(),
            data.pendingRewards?.toString() || null,
            data.pendingRewardsDiff?.toString() || null,
            data.investmentData,
          ]),
        ],
        options.ctx.client,
      );
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}

interface DbInsertRewards {
  datetime: Date;
  blockNumber: number;
  productId: number;
  investorId: number;
  balance: Decimal;
  pendingRewards: Decimal | null;
  pendingRewardsDiff: Decimal | null;
  investmentData: object;
}

export function upsertInvestmentRewards$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbInsertRewards>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getInvestmentData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, insert: DbInsertRewards) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getInvestmentData,
    logInfos: { msg: "upsertInvestmentRewards" },
    processBatch: async (objAndData) => {
      await db_query(
        `INSERT INTO investment_balance_ts (
              datetime,
              block_number,
              product_id,
              investor_id,
              balance,
              balance_diff,
              pending_rewards,
              pending_rewards_diff,
              investment_data
          ) VALUES %L
              ON CONFLICT (product_id, investor_id, block_number, datetime) 
              DO UPDATE SET 
                pending_rewards = coalesce(investment_balance_ts.pending_rewards, EXCLUDED.pending_rewards),
                pending_rewards_diff = coalesce(investment_balance_ts.pending_rewards_diff, EXCLUDED.pending_rewards_diff),
                investment_data = jsonb_merge(investment_balance_ts.investment_data, EXCLUDED.investment_data)
          `,
        [
          objAndData.map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.productId,
            data.investorId,
            data.balance.toString(),
            "0", // balance_diff
            data.pendingRewards?.toString() || null,
            data.pendingRewardsDiff?.toString() || null,
            data.investmentData,
          ]),
        ],
        options.ctx.client,
      );
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}

export function fetchAllInvestorIds$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends number>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getProductId: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investorIds: number[]) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getProductId,
    logInfos: { msg: "fetchAllInvestorIds" },
    processBatch: async (objAndData) => {
      const res = await db_query<{ product_id: number; investor_id: number }>(
        `select distinct product_id, investor_id 
        from investment_balance_ts
        where product_id in (%L)`,
        [objAndData.map(({ data }) => data)],
        options.ctx.client,
      );
      const resMap = groupBy(res, (row) => row.product_id);
      return new Map(objAndData.map(({ data: productId }) => [productId, resMap[productId]?.map((r) => r.investor_id) || []]));
    },
  });
}
