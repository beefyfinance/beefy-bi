import { keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";

interface DbInvestor {
  investorId: number;
  address: string;
  investorData: {};
}

export function upsertInvestor$<TObj, TParams extends Omit<DbInvestor, "investorId">, TRes>(options: {
  client: PoolClient;
  getInvestorData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investorId: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }
      const objAndData = objs.map((obj) => ({ obj, investorData: options.getInvestorData(obj) }));

      type TRes = { investor_id: number; address: string };
      const results = await db_query<TRes>(
        `INSERT INTO investor (address, investor_data) VALUES %L
          ON CONFLICT (address) DO UPDATE SET investor_data = jsonb_merge(investor.investor_data, EXCLUDED.investor_data)
          RETURNING investor_id, bytea_to_hexstr(address) as address`,
        [
          uniqBy(objAndData, (objAndData) => objAndData.investorData.address.toLocaleLowerCase()).map((obj) => [
            strAddressToPgBytea(obj.investorData.address),
            obj.investorData.investorData,
          ]),
        ],
        options.client,
      );

      const idMap = keyBy(results, "address");
      return objAndData.map((obj) => {
        const investor = idMap[obj.investorData.address.toLocaleLowerCase()];
        if (!investor) {
          throw new ProgrammerError({ msg: "Upserted investor not found", data: obj });
        }
        return options.formatOutput(obj.obj, investor.investor_id);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}
