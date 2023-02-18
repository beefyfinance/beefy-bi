import { deepEqual } from "assert";
import { keyBy, uniq, uniqBy } from "lodash";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

export interface DbInvestor {
  investorId: number;
  address: string;
  investorData: {};
}

export function upsertInvestor$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends Omit<DbInvestor, "investorId">>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getInvestorData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investorId: number) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getInvestorData,
    logInfos: { msg: "upsert investor" },
    processBatch: async (objAndData) => {
      // select existing investors to update them instead of inserting new ones so we don't burn too much serial ids
      // More details: https://dba.stackexchange.com/a/295356/65636
      const existingInvestors = await db_query<DbInvestor>(
        `SELECT investor_id as "investorId", bytea_to_hexstr(address) as "address", investor_data as "investorData"
         FROM investor WHERE address IN (%L)`,
        [uniq(objAndData.map(({ data }) => strAddressToPgBytea(data.address)))],
        options.ctx.client,
      );

      const existingInvestorsByAddress = keyBy(existingInvestors, "address");
      const investorsToInsert = objAndData.filter(({ data }) => !existingInvestorsByAddress[data.address.toLocaleLowerCase()]);
      const investorsToUpdate = objAndData.filter(({ data }) => existingInvestorsByAddress[data.address.toLocaleLowerCase()]);

      const insertResults =
        investorsToInsert.length <= 0
          ? []
          : await db_query<DbInvestor>(
              `INSERT INTO investor (address, investor_data) VALUES %L
                ON CONFLICT (address) DO UPDATE SET investor_data = jsonb_merge(investor.investor_data, EXCLUDED.investor_data)
                RETURNING investor_id as "investorId", bytea_to_hexstr(address) as "address", investor_data as "investorData"`,
              [
                uniqBy(investorsToInsert, (objAndData) => objAndData.data.address.toLocaleLowerCase()).map((obj) => [
                  strAddressToPgBytea(obj.data.address),
                  obj.data.investorData,
                ]),
              ],
              options.ctx.client,
            );
      const updateResults =
        investorsToUpdate.length <= 0
          ? []
          : await db_query<DbInvestor>(
              `UPDATE investor 
                SET investor_data = jsonb_merge(investor.investor_data, u.investor_data)
                FROM (VALUES %L) AS u(address, investor_data)
                WHERE investor.address = u.address::bytea
                RETURNING investor_id as "investorId", bytea_to_hexstr(investor.address) as "address", investor.investor_data as "investorData"`,
              [
                uniqBy(investorsToUpdate, (objAndData) => objAndData.data.address.toLocaleLowerCase()).map((obj) => [
                  strAddressToPgBytea(obj.data.address),
                  obj.data.investorData,
                ]),
              ],
              options.ctx.client,
            );
      const results = [...insertResults, ...updateResults];

      // return results in the same order
      const idMap = keyBy(results, "address");
      return new Map(
        objAndData.map(({ data }) => {
          const investor = idMap[data.address.toLocaleLowerCase()];
          if (!investor) {
            throw new ProgrammerError({ msg: "Upserted investor not found", data });
          }
          return [data, investor.investorId];
        }),
      );
    },
  });
}

export function fetchInvestor$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends number>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getInvestorId: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investor: DbInvestor) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getInvestorId,
    logInfos: { msg: "fetch investor" },
    processBatch: async (objAndData) => {
      const results = await db_query<DbInvestor>(
        `SELECT
            investor_id as "investorId", 
            bytea_to_hexstr(address) as "address", 
            investor_data as "investorData"
        FROM investor
        WHERE investor_id IN (%L)`,
        [uniq(objAndData.map(({ data }) => data))],
        options.ctx.client,
      );

      // return a map where keys are the original parameters object refs
      const idMap = keyBy(results, "investorId");

      return new Map(
        objAndData.map(({ data }) => {
          const investor = idMap[data];
          if (!investor) {
            throw new Error("Could not find investor");
          }
          return [data, investor];
        }),
      );
    },
  });
}
