import Decimal from "decimal.js";
import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

export interface DbInvestment {
  datetime: Date;
  productId: number;
  investorId: number;
  balance: Decimal;
  investmentData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertInvestment(client: PoolClient): Rx.OperatorFunction<DbInvestment, DbInvestment> {
  return Rx.pipe(
    Rx.bufferCount(500),

    // insert to the investment table
    Rx.mergeMap(async (investments) => {
      // short circuit if there's nothing to do
      if (investments.length === 0) {
        return [];
      }

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
          investments.map((investement) => [
            investement.datetime.toISOString(),
            investement.productId,
            investement.investorId,
            investement.balance.toString(),
            investement.investmentData,
          ]),
        ],
        client,
      );
      return investments;
    }),

    Rx.mergeMap((investments) => Rx.from(investments)), // flatten
  );
}
