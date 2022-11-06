import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { SamplingPeriod } from "../../../types/sampling";
import { db_query, db_query_one } from "../../../utils/db";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { ErrorEmitter, ImportCtx } from "../types/import-context";

export function fetchChainBlockList$<
  TObj,
  TErr extends ErrorEmitter<TObj>,
  TRes,
  TListItem extends { datetime: Date; block_number: number | null; interpolated_block_number: number },
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getFirstDate: (obj: TObj) => Date;
  timeStep: SamplingPeriod;
  getChain: (obj: TObj) => Chain;
  formatOutput: (obj: TObj, blockList: TListItem[]) => TRes;
}) {
  const operator$ = Rx.pipe(
    Rx.map((obj: TObj) => ({ obj })),
    // fetch the first date for this chain
    Rx.concatMap(async (item) => {
      const firstChainDate = await db_query_one<{ first_date: Date | null }>(
        `
          select min(datetime) as first_date
          from investment_balance_ts
          where product_id in (
                select product_id
                from product
                where chain = %L
            )
      `,
        [options.getChain(item.obj)],
        options.ctx.client,
      );
      return { ...item, firstChainDate: firstChainDate?.first_date || new Date() };
    }),

    // fetch a decent first list of blocks from investments of this chain
    Rx.concatMap(async (item) => {
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
        [options.timeStep, item.firstChainDate.toISOString(), options.timeStep, options.timeStep, options.getChain(item.obj)],
        options.ctx.client,
      );
      return { input: item.obj, output: blockList };
    }),
  );

  return Rx.pipe(
    cacheOperatorResult$({
      operator$,
      getCacheKey: (item) => `${options.getChain(item)}`,
      logInfos: { msg: "fetchChainBlockList$" },
      stdTTLSec: 5 * 60 /* 5min */,
      formatOutput: (obj, blockList) => ({ obj, blockList }),
    }),

    // filter the block list by the requested first date
    Rx.map((item) => {
      const afterThisDate = options.getFirstDate(item.obj);
      const filteredBlockList = item.blockList.filter((block) => block.datetime >= afterThisDate);
      return options.formatOutput(item.obj, filteredBlockList);
    }),
  );
}
