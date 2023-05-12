import { keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

interface DbBlock {
  datetime: Date;
  chain: Chain;
  blockNumber: number;
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
            block_number
        ) VALUES %L
            ON CONFLICT (block_number, chain, datetime) 
            DO NOTHING
        `,
        [
          uniqBy(objAndData, ({ data }) => `${options.ctx.chain}-${data.blockNumber}-${data.datetime.toISOString()}`).map(({ data }) => [
            data.datetime.toISOString(),
            data.chain,
            data.blockNumber,
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
  formatOutput: (obj: TObj, block: Omit<DbBlock, "blockData"> | null) => TRes;
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
            block_number as "blockNumber"
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
