import Decimal from "decimal.js";
import { groupBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "common", component: "investment" });

export interface DbInvestment {
  datetime: Date;
  blockNumber: number;
  productId: number;
  investorId: number;
  balance: Decimal;
  investmentData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertInvestment$<TInput, TRes>(options: {
  client: PoolClient;
  getInvestmentData: (obj: TInput) => DbInvestment;
  formatOutput: (obj: TInput, investment: DbInvestment) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // insert to the investment table
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, investment: options.getInvestmentData(obj) }));

      // add duplicate detection in dev only
      if (process.env.NODE_ENV === "development") {
        const duplicates = Object.entries(
          groupBy(objAndData, ({ investment }) => `${investment.productId}-${investment.investorId}-${investment.blockNumber}`),
        ).filter(([_, v]) => v.length > 1);
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
          objAndData.map(({ investment }) => [
            investment.datetime.toISOString(),
            investment.blockNumber,
            investment.productId,
            investment.investorId,
            investment.balance.toString(),
            investment.investmentData,
          ]),
        ],
        options.client,
      );
      return objAndData.map(({ obj, investment }) => options.formatOutput(obj, investment));
    }),

    Rx.concatMap((investments) => investments), // flatten
  );
}
