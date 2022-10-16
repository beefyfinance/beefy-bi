import Decimal from "decimal.js";
import { groupBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { SupportedRangeTypes } from "../../../utils/range";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
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

export function upsertInvestment$<
  TTarget,
  TRange extends SupportedRangeTypes,
  TParams extends DbInvestment,
  TObj extends ImportQuery<TTarget, TRange>,
  TRes extends ImportQuery<TTarget, TRange>,
>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter<TTarget, TRange>;
  getInvestmentData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbInvestment) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return dbBatchCall$({
    client: options.client,
    streamConfig: options.streamConfig,
    emitErrors: options.emitErrors,
    formatOutput: options.formatOutput,
    getData: options.getInvestmentData,
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
        options.client,
      );
      return objAndData.map(({ data }) => data);
    },
  });
}
