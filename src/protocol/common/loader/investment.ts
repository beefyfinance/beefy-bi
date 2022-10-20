import Decimal from "decimal.js";
import { groupBy } from "lodash";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "common", component: "investment" });

export interface DbInvestment {
  datetime: Date;
  blockNumber: number;
  productId: number;
  investorId: number;
  balance: Decimal;
  investmentData: object;
}

export function upsertInvestment$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends DbInvestment>(options: {
  ctx: TCtx;
  getInvestmentData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbInvestment) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    formatOutput: options.formatOutput,
    getData: options.getInvestmentData,
    logInfos: { msg: "upsertInvestment" },
    processBatch: async (objAndData) => {
      // add duplicate detection in dev only
      if (process.env.NODE_ENV === "development") {
        const duplicates = Object.entries(groupBy(objAndData, ({ data }) => `${data.productId}-${data.investorId}-${data.blockNumber}`)).filter(
          ([_, v]) => v.length > 1,
        );
        if (duplicates.length > 0) {
          logger.error({ msg: "Duplicate investments", data: duplicates });
        }
      }

      await db_query(
        `INSERT INTO investment_balance_ts (
              datetime,
              block_number,
              product_id,
              investor_id,
              balance,
              investment_data
          ) VALUES %L
              ON CONFLICT (product_id, investor_id, block_number, datetime) 
              DO UPDATE SET 
                balance = EXCLUDED.balance, 
                investment_data = jsonb_merge(investment_balance_ts.investment_data, EXCLUDED.investment_data)
          `,
        [
          objAndData.map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.productId,
            data.investorId,
            data.balance.toString(),
            data.investmentData,
          ]),
        ],
        options.ctx.client,
      );
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}
