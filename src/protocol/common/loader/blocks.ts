import { uniqBy } from "lodash";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

export interface DbBlock {
  datetime: Date;
  chain: Chain;
  blockNumber: number;
  blockData: object;
}

// upsert the address of all objects and return the id in the specified field
export function upsertBlock$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends DbBlock>(options: {
  ctx: TCtx;
  getBlockData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, block: DbBlock) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
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
