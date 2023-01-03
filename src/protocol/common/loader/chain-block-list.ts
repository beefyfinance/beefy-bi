import { groupBy, keyBy, min, uniq } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { SamplingPeriod } from "../../../types/sampling";
import { db_query, db_query_one } from "../../../utils/db";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

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
    dbBatchCall$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      getData: (item) => options.getChain(item.obj),
      logInfos: { msg: "fetchChainBlockList$.minDate" },
      processBatch: async (objAndData) => {
        const firstDateResults = await db_query<{ chain: Chain; first_date: Date }>(
          `
            select chain, min(datetime) as first_date
            from block_ts
            where chain in (%L)
            group by 1
        `,
          [uniq(objAndData.map(({ data }) => data))],
          options.ctx.client,
        );
        const firstDateIdMap = keyBy(firstDateResults, "chain");

        const blockList = await db_query<TListItem>(
          `
              with blocks as (
                select 
                    chain,
                    time_bucket_gapfill(%L, datetime) as datetime,
                    last(block_number, datetime) as block_number,
                    interpolate(last(block_number, datetime)) as interpolated_block_number
                from block_ts
                where 
                    datetime between (%L::timestamptz - %L::interval) and (now() - %L::interval)
                    and chain in (%L)
                group by 1, 2
              ) 
              select * 
              from blocks
              where interpolated_block_number is not null;
          `,
          [
            options.timeStep,
            min(objAndData.map(({ data }) => firstDateIdMap[data]?.first_date ?? new Date()))?.toISOString(),
            options.timeStep,
            options.timeStep,
            uniq(objAndData.map(({ data }) => data)),
          ],
          options.ctx.client,
        );
        const blockListMap = groupBy(blockList, "chain");

        return new Map(objAndData.map(({ data }) => [data, blockListMap[data] ?? []]));
      },
      formatOutput: (item, blockList) => ({ ...item, blockList }),
    }),

    // format for caching
    Rx.map((item) => ({ input: item.obj, output: item.blockList })),
  );

  return Rx.pipe(
    cacheOperatorResult$({
      operator$,
      cacheConfig: {
        type: "global",
        globalKey: "chain-block-list",
        stdTTLSec: 5 * 60 /* 5min */,
        useClones: false, // we can't afford to clone the block list and we know it's immutable
      },
      getCacheKey: (item) => `chain-block-list:${options.getChain(item)}:${options.timeStep}`,
      logInfos: { msg: "fetchChainBlockList$" },
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
