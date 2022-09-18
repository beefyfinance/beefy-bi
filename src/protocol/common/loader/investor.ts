import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

interface DbInvestor {
  investorId: number;
  investorAddress: string;
  investorData: {};
}

// upsert the address of all objects and return the id in the specified field
export function upsertInvestor<TObj, TParams extends Omit<DbInvestor, "investorId">, TRes>(options: {
  client: PoolClient;
  getInvestorData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investorId: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchQueryGroup({
    bufferCount: 500,
    processBatch: async (params: TParams[]) => {
      type TRes = { investor_id: number; address: string };
      const results = await db_query<TRes>(
        `INSERT INTO investor (investor_address, investor_data) VALUES %L
          ON CONFLICT (investor_address) DO UPDATE SET investor_data = jsonb_merge(investor.investor_data, EXCLUDED.investor_data)
          RETURNING investor_id, bytea_to_hexstr(investor_address) as address`,
        [params.map(({ investorAddress, investorData }) => [strAddressToPgBytea(investorAddress), investorData])],
        options.client,
      );
      // ensure results are in the same order as the params
      const idMap = keyBy(results, "address");
      return params.map((param) => idMap[param.investorAddress.toLocaleLowerCase()].investor_id);
    },
    toQueryObj: (objs) => options.getInvestorData(objs[0]),
    getBatchKey: (obj) => {
      const params = options.getInvestorData(obj);
      return params.investorAddress.toLocaleLowerCase();
    },
    formatOutput: options.formatOutput,
  });
}
