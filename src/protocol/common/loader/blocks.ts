import { isEmpty, keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { v4 as uuid } from "uuid";
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
      // generate debug data uuid for each object
      const objAndDataAndUuid = objAndData.map(({ obj, data }) => ({ obj, data, debugDataUuid: uuid() }));

      const upsertPromise = db_query(
        `INSERT INTO block_ts (
            datetime,
            chain,
            block_number,
            debug_data_uuid
        ) VALUES %L
            ON CONFLICT (block_number, chain, datetime) 
            DO UPDATE SET 
            debug_data_uuid = coalesce(block_ts.debug_data_uuid, EXCLUDED.debug_data_uuid)
        `,
        [
          uniqBy(objAndDataAndUuid, ({ data }) => `${options.ctx.chain}-${data.blockNumber}-${data.datetime.toISOString()}`).map(
            ({ data, debugDataUuid }) => [data.datetime.toISOString(), data.chain, data.blockNumber, debugDataUuid],
          ),
        ],
        options.ctx.client,
      );

      const debugData = objAndDataAndUuid
        .filter(({ data }) => !isEmpty(data.blockData)) // don't insert empty data
        .map(({ data, debugDataUuid }) => [debugDataUuid, data.datetime.toISOString(), "block_ts", data.blockData]);

      const debugPromise =
        debugData.length <= 0
          ? Promise.resolve()
          : db_query(`INSERT INTO debug_data_ts (debug_data_uuid, datetime, origin_table, debug_data) VALUES %L`, [debugData], options.ctx.client);

      await Promise.all([upsertPromise, debugPromise]);
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
