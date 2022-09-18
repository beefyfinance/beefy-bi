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
export function upsertInvestor<TObj, TKey extends string, TParams extends Omit<DbInvestor, "investorId">>(
  client: PoolClient,
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj, TObj & { [key in TKey]: number }> {
  const toQueryObj = (obj: TObj[]) => getParams(obj[0]);
  const getKeyFromObj = (obj: TObj) => getKeyFromParams(getParams(obj));
  const getKeyFromParams = ({ investorAddress }: TParams) => {
    return `${investorAddress.toLocaleLowerCase()}`;
  };
  const process = async (params: TParams[]) => {
    type TRes = { investor_id: number; address: string };
    const results = await db_query<TRes>(
      `INSERT INTO investor (investor_address, investor_data) VALUES %L
        ON CONFLICT (investor_address) DO UPDATE SET investor_data = jsonb_merge(investor.investor_data, EXCLUDED.investor_data)
        RETURNING investor_id, bytea_to_hexstr(investor_address) as address`,
      [params.map(({ investorAddress, investorData }) => [strAddressToPgBytea(investorAddress), investorData])],
      client,
    );
    // ensure results are in the same order as the params
    const idMap = keyBy(results, "address");
    return params.map((param) => idMap[param.investorAddress.toLocaleLowerCase()].investor_id);
  };

  return Rx.pipe(
    // betch queries
    Rx.bufferCount(500),

    batchQueryGroup(toQueryObj, getKeyFromObj, process, toKey),

    // flatten objects
    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}
