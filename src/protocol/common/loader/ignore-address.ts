import { uniqBy } from "lodash";
import { Chain } from "../../../types/chain";
import { db_query, strAddressToPgBytea } from "../../../utils/db";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

export interface DbIgnoreAddress {
  chain: Chain;
  address: string;
}

export function upsertIgnoreAddress$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbIgnoreAddress>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getIgnoreAddressData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, ignoreAddress: DbIgnoreAddress) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getIgnoreAddressData,
    logInfos: { msg: "upsert ignore address data" },
    processBatch: async (objAndData) => {
      await db_query(
        `INSERT INTO ignore_address (chain, address) VALUES %L 
        ON CONFLICT (chain, address) DO NOTHING`,
        [uniqBy(objAndData, ({ data }) => `${data.chain}:${data.address}`).map((obj) => [obj.data.chain, strAddressToPgBytea(obj.data.address)])],
        options.ctx.client,
      );

      return new Map(objAndData.map(({ data }) => [data, { chain: data.chain, address: data.address }]));
    },
  });
}
