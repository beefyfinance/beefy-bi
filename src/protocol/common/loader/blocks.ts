import { keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

export interface DbBlock {
  datetime: Date;
  chain: Chain;
  blockNumber: number;
  blockData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertBlock$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbBlock>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getBlockData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, block: DbBlock) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getBlockData,
    logInfos: { msg: "upsert block" },
    processBatch: async (objAndData) => {
      await db_query(
        `INSERT INTO block_ts (
              datetime,
              chain,
              block_number,
              block_data
          ) VALUES %L
              ON CONFLICT (block_number, chain, datetime) 
              DO UPDATE SET 
              block_data = jsonb_merge(block_ts.block_data, EXCLUDED.block_data)
          `,
        [
          uniqBy(objAndData, ({ data }) => `${data.blockNumber}-${data.datetime.toISOString()}`).map(({ data }) => [
            data.datetime.toISOString(),
            data.chain,
            data.blockNumber,
            data.blockData,
          ]),
        ],
        options.ctx.client,
      );
      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}

export function fetchBlock$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  chain: Chain;
  getBlockNumber: (obj: TObj) => number;
  formatOutput: (obj: TObj, block: DbBlock | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    getData: options.getBlockNumber,
    formatOutput: options.formatOutput,
    logInfos: { msg: "fetch block" },
    processBatch: async (objAndData) => {
      const results = await db_query<DbBlock>(
        `SELECT 
            datetime,
            chain,
            block_number as "blockNumber",
            block_data as "blockData"
          FROM block_ts
          WHERE chain = %L and block_number IN (%L)`,
        [options.chain, objAndData.map((obj) => obj.data)],
        options.ctx.client,
      );

      // return a map where keys are the original parameters object refs
      const idMap = keyBy(results, "blockNumber");
      return new Map(objAndData.map(({ data }) => [data, idMap[data] ?? null]));
    },
  });
}
