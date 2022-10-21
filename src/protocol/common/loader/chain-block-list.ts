import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { db_query } from "../../../utils/db";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { ImportCtx } from "../types/import-context";

export function fetchChainBlockList$<
  TObj,
  TRes,
  TListItem extends { datetime: Date; block_number: number | null; interpolated_block_number: number },
>(options: {
  ctx: ImportCtx<TObj>;
  getFirstDate: (obj: TObj) => Date;
  getChain: (obj: TObj) => Chain;
  formatOutput: (obj: TObj, blockList: TListItem[]) => TRes;
}) {
  const operator$ = Rx.pipe(
    // fetch a decent first list of blocks from investments of this chain
    Rx.concatMap(async (item: TObj) => {
      const timeStep = "15 minutes";
      const blockList = await db_query<TListItem>(
        `
            with blocks as (
              select 
                  time_bucket_gapfill(%L, datetime) as datetime,
                  last(block_number, datetime) as block_number,
                  interpolate(last(block_number, datetime)) as interpolated_block_number
              from investment_balance_ts
              where 
                  datetime between (%L::timestamptz - %L::interval) and (now() - %L::interval)
                  and product_id in (
                      select product_id
                      from product
                      where chain = %L
                  )
              group by 1
            ) 
            select * 
            from blocks
            where interpolated_block_number is not null;
        `,
        [timeStep, options.getFirstDate(item).toISOString(), timeStep, timeStep, options.getChain(item)],
        options.ctx.client,
      );

      return { input: item, output: blockList };
    }),
  );

  return cacheOperatorResult$({
    operator$,
    getCacheKey: (item) => `${options.getChain(item)}-${options.getFirstDate(item).toISOString()}`,
    logInfos: { msg: "fetchChainBlockList$" },
    stdTTLSec: 5 * 60 /* 5min */,
    formatOutput: (item, result) => options.formatOutput(item, result),
  });
}
