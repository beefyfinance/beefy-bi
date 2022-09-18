import Decimal from "decimal.js";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";

export interface DbInvestment {
  datetime: Date;
  productId: number;
  investorId: number;
  balance: Decimal;
  investmentData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertInvestment<TInput, TRes>(options: {
  client: PoolClient;
  getInvestmentData: (obj: TInput) => DbInvestment;
  formatOutput: (obj: TInput, investment: DbInvestment) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferCount(500),

    // insert to the investment table
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, investment: options.getInvestmentData(obj) }));

      await db_query(
        `INSERT INTO user_investment_ts (
              datetime,
              product_id,
              investor_id,
              balance,
              investment_data
          ) VALUES %L
              ON CONFLICT (product_id, investor_id, datetime) 
              DO UPDATE SET 
                balance = EXCLUDED.balance, 
                investment_data = jsonb_merge(user_investment_ts.investment_data, EXCLUDED.investment_data)
          `,
        [
          objAndData.map(({ investment }) => [
            investment.datetime.toISOString(),
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

    Rx.mergeMap((investments) => Rx.from(investments)), // flatten
  );
}
