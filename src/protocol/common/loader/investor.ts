import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

interface DbInvestor {
  investorId: number;
  address: string;
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
        `INSERT INTO investor (address, investor_data) VALUES %L
          ON CONFLICT (address) DO UPDATE SET investor_data = jsonb_merge(investor.investor_data, EXCLUDED.investor_data)
          RETURNING investor_id, bytea_to_hexstr(address) as address`,
        [params.map(({ address, investorData }) => [strAddressToPgBytea(address), investorData])],
        options.client,
      );
      // ensure results are in the same order as the params
      const idMap = keyBy(results, "address");
      return params.map((param) => idMap[param.address.toLocaleLowerCase()].investor_id);
    },
    toQueryObj: (objs) => options.getInvestorData(objs[0]),
    getBatchKey: (obj) => {
      const params = options.getInvestorData(obj);
      return params.address.toLocaleLowerCase();
    },
    formatOutput: options.formatOutput,
  });
}
