import Decimal from "decimal.js";
import { groupBy, isEqual } from "lodash";
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
      const groups = Object.entries(
        groupBy(objAndData, ({ data }) => `${data.productId}-${data.investorId}-${data.blockNumber}-${data.datetime.toISOString()}`),
      ).map(([_, objsAndData]) => {
        // remove exact duplicates using lodash isEqual (deep comparison)
        const uniqObjsAndData = objsAndData.filter((objAndData, index) => {
          // only keep the first occurrence of each object
          return (
            objsAndData.findIndex((objAndData2) =>
              isEqual(
                {
                  balance: objAndData2.data.balance.toString(),
                  balance_diff: objAndData2.data.balanceDiff.toString(),
                  investment_data: objAndData2.data.investmentData,
                },

                {
                  balance: objAndData.data.balance.toString(),
                  balance_diff: objAndData.data.balanceDiff.toString(),
                  investment_data: objAndData.data.investmentData,
                },
              ),
            ) === index
          );
        });
        return uniqObjsAndData;
      });

      const duplicates = groups.filter((objsAndData) => objsAndData.length > 1);
      if (duplicates.length > 0) {
        logger.error({ msg: "Duplicate investments", data: duplicates.map((d) => d.map((d) => d.data.investmentData)) });
        throw new Error("Duplicate investments");
      }

      const uniqObjsAndData = groups.flat();

      await db_query(
        `INSERT INTO investment_balance_ts (
              datetime,
              block_number,
              product_id,
              investor_id,
              balance,
              balance_diff,
              investment_data
          ) VALUES %L
              ON CONFLICT (product_id, investor_id, block_number, datetime) 
              DO UPDATE SET 
                balance = EXCLUDED.balance, 
                balance_diff = EXCLUDED.balance_diff,
                investment_data = jsonb_merge(investment_balance_ts.investment_data, EXCLUDED.investment_data)
          `,
        [
          uniqObjsAndData.map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.productId,
            data.investorId,
            data.balance.toString(),
            data.balanceDiff.toString(),
            data.investmentData,
          ]),
        ],
        options.ctx.client,
      );
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}
