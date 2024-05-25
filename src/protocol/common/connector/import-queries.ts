import * as Rx from "rxjs";
import { samplingPeriodMs } from "../../../types/sampling";
import { Range, SupportedRangeTypes, rangeSortedArrayExclude, rangeSortedSplitManyToMaxLengthAndTakeSome } from "../../../utils/range";
import { DbDateRangeImportState, DbImportState } from "../loader/import-state";
import { ImportBehaviour, ImportCtx } from "../types/import-context";

export function addHistoricalDateQuery$<TObj, TRes, TImport extends DbDateRangeImportState>(options: {
  ctx: ImportCtx;
  getImport: (obj: TObj) => TImport;
  getFirstDate: (importState: TImport) => Date;
  formatOutput: (obj: TObj, latestDate: Date, historicalDateQueries: Range<Date>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // we can now create the historical block query
    Rx.map((item) => {
      const importState = options.getImport(item);
      const maxMsPerQuery = samplingPeriodMs[options.ctx.behaviour.beefyPriceDataQueryRange];
      const latestDate = new Date();

      // this is the whole range we have to cover
      let fullRange = options.ctx.behaviour.forceConsideredDateRange
        ? options.ctx.behaviour.forceConsideredDateRange
        : {
            from: options.getFirstDate(importState),
            to: latestDate,
          };

      let ranges = [fullRange];

      ranges = _restrictRangesWithImportState(
        options.ctx.behaviour,
        ranges,
        importState,
        maxMsPerQuery,
        options.ctx.behaviour.limitQueriesCountTo.price,
      );
      return options.formatOutput(item, latestDate, ranges);
    }),
  );
}

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestDateQuery$<TObj, TRes>(options: {
  ctx: ImportCtx;
  getLastImportedDate: () => Date | null;
  formatOutput: (obj: TObj, latestDate: Date, recentDateQuery: Range<Date>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.map((item) => {
      const latestDate = new Date();
      const maxMsPerQuery = samplingPeriodMs[options.ctx.behaviour.beefyPriceDataQueryRange];
      const lastImportedDate = options.getLastImportedDate() || new Date(0);
      const fromMs = Math.max(lastImportedDate.getTime(), latestDate.getTime() - maxMsPerQuery);
      const recentDateQuery = {
        from: new Date(fromMs),
        to: latestDate,
      };
      return options.formatOutput(item, latestDate, recentDateQuery);
    }),
  );
}

export function _restrictRangesWithImportState<T extends SupportedRangeTypes>(
  behaviour: ImportBehaviour,
  ranges: Range<T>[],
  importState: DbImportState,
  maxRangeLength: number,
  limitRangeCount: number,
): Range<T>[] {
  if (!behaviour.ignoreImportState) {
    // exclude covered ranges and retry ranges
    const toExclude = [...(importState.importData.ranges.coveredRanges as Range<T>[]), ...(importState.importData.ranges.toRetry as Range<T>[])];

    // exclude the ranges we already covered
    ranges = rangeSortedArrayExclude(ranges, toExclude);
  }

  // split in ranges no greater than the maximum allowed
  // order by new range first since it's more important and more likely to be available via RPC calls
  ranges = rangeSortedSplitManyToMaxLengthAndTakeSome(ranges, maxRangeLength, limitRangeCount, "desc");

  // if there is room, add the ranges that failed to be imported
  if (ranges.length < limitRangeCount) {
    let rangesToRetry = importState.importData.ranges.toRetry as Range<T>[];
    rangesToRetry = rangeSortedSplitManyToMaxLengthAndTakeSome(rangesToRetry, maxRangeLength, limitRangeCount - ranges.length, "desc");

    // put retries last
    ranges = ranges.concat(rangesToRetry);
  }
  // limit the amount of queries sent
  if (ranges.length > limitRangeCount) {
    ranges = ranges.slice(0, limitRangeCount);
  }
  return ranges;
}
