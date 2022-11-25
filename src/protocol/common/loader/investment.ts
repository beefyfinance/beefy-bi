import Decimal from "decimal.js";
import { groupBy, keyBy, merge } from "lodash";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
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
              investmentData: group.reduce((acc, objAndData) => merge(acc, objAndData.data.investmentData), {}),
            },
          });
        } else {
          logger.error({ msg: "upsertInvestment: all investments are not the same, not merging them", data: group });
          throw new ProgrammerError("upsertInvestment: all investments are not the same, cannot merge them");
        }
      }

      const results = await db_query<{ product_id: number; investor_id: number; block_number: number }>(
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
          RETURNING product_id, investor_id, block_number
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
            data.investmentData,
          ]),
        ],
        options.ctx.client,
      );

      const idMap = keyBy(results, (result) => `${result.product_id}:${result.investor_id}:${result.block_number}`);
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
