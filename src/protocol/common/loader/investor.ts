import { keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { ProgrammerError } from "../../../utils/programmer-error";
import { SupportedRangeTypes } from "../../../utils/range";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { dbBatchCall$ } from "../utils/db-batch";

interface DbInvestor {
  investorId: number;
  address: string;
  investorData: {};
}

export function upsertInvestor$<
  TTarget,
  TRange extends SupportedRangeTypes,
  TParams extends Omit<DbInvestor, "investorId">,
  TObj extends ImportQuery<TTarget, TRange>,
  TRes extends ImportQuery<TTarget, TRange>,
>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  emitErrors: ErrorEmitter<TTarget, TRange>;
  getInvestorData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investorId: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return dbBatchCall$({
    client: options.client,
    streamConfig: options.streamConfig,
    emitErrors: options.emitErrors,
    formatOutput: options.formatOutput,
    getData: options.getInvestorData,
    processBatch: async (objAndData) => {
      const results = await db_query<{ investor_id: number; address: string }>(
        `INSERT INTO investor (address, investor_data) VALUES %L
        ON CONFLICT (address) DO UPDATE SET investor_data = jsonb_merge(investor.investor_data, EXCLUDED.investor_data)
        RETURNING investor_id, bytea_to_hexstr(address) as address`,
        [
          uniqBy(objAndData, (objAndData) => objAndData.data.address.toLocaleLowerCase()).map((obj) => [
            strAddressToPgBytea(obj.data.address),
            obj.data.investorData,
          ]),
        ],
        options.client,
      );

      // return results in the same order
      const idMap = keyBy(results, "address");
      return objAndData.map((obj) => {
        const investor = idMap[obj.data.address.toLocaleLowerCase()];
        if (!investor) {
          throw new ProgrammerError({ msg: "Upserted investor not found", data: obj });
        }
        return investor.investor_id;
      });
    },
  });
}
