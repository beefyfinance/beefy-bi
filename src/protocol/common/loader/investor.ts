import { keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

interface DbInvestor {
  investorId: number;
  address: string;
  investorData: {};
}

export function upsertInvestor$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends Omit<DbInvestor, "investorId">>(options: {
  ctx: TCtx;
  getInvestorData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investorId: number) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
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
        options.ctx.client,
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
