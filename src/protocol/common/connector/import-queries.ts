import { max, min, sortBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import { MS_PER_BLOCK_ESTIMATE } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import {
  Range,
  rangeArrayExclude,
  rangeSortedArrayExclude,
  rangeSortedSplitManyToMaxLengthAndTakeSome,
  SupportedRangeTypes,
} from "../../../utils/range";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { fetchChainBlockList$ } from "../loader/chain-block-list";
import { DbBlockNumberRangeImportState, DbDateRangeImportState, DbImportState } from "../loader/import-state";
import { ErrorEmitter, ImportBehavior, ImportCtx } from "../types/import-context";
import { latestBlockNumber$ } from "./latest-block-number";

const logger = rootLogger.child({ module: "common", component: "import-queries" });

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestBlockQuery$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getLastImportedBlock: (chain: Chain) => number | null;
  formatOutput: (obj: TObj, latestBlockNumber: number, latestBlockQuery: Range<number>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // set the input type
    Rx.tap((_: TObj) => {}),

    Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, options.ctx.streamConfig.maxInputTake),
    Rx.filter((items) => items.length > 0),

    // go get the latest block number for this chain
    latestBlockNumber$({
      ctx: options.ctx,
      emitError: (items, report) => items.map((item) => options.emitError(item, report)),
      formatOutput: (objs, latestBlockNumber) => ({ objs, latestBlockNumber }),
    }),

    // compute the block range we want to query
    Rx.mergeMap((objGroup) => {
      // fetch the last hour of data
      const maxBlocksPerQuery = options.ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
      const period = samplingPeriodMs["1hour"];
      const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[options.ctx.chain]);

      const lastImportedBlockNumber = options.getLastImportedBlock(options.ctx.chain);
      const diffBetweenLastImported = lastImportedBlockNumber ? objGroup.latestBlockNumber - (lastImportedBlockNumber + 1) : Infinity;

      const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
      const fromBlock = objGroup.latestBlockNumber - blockCountToFetch;
      const toBlock = objGroup.latestBlockNumber;

      const query = {
        from: fromBlock - options.ctx.behavior.waitForBlockPropagation,
        to: toBlock - options.ctx.behavior.waitForBlockPropagation,
      };
      logger.trace({
        msg: "latest block query generated",
        data: { query, fromBlock, toBlock, maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported, blockCountToFetch },
      });
      return objGroup.objs.map((obj) => options.formatOutput(obj, objGroup.latestBlockNumber, query));
    }, options.ctx.streamConfig.workConcurrency),
  );
}

export function addHistoricalBlockQuery$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TImport extends DbBlockNumberRangeImportState>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  isLiveItem: (input: TObj) => boolean;
  getImport: (obj: TObj) => TImport;
  getFirstBlockNumber: (importState: TImport) => number;
  formatOutput: (obj: TObj, latestBlockNumber: number, historicalBlockQueries: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // get the recent data query to avoid double fetching it
    addLatestBlockQuery$({
      ctx: options.ctx,
      emitError: options.emitError,
      getLastImportedBlock: () => null,
      formatOutput: (obj, latestBlockNumber, latestBlockQuery) => ({ obj, latestBlockNumber, latestBlockQuery }),
    }),

    // we can now create the historical block query
    Rx.map((item) => {
      const importState = options.getImport(item.obj);

      // this is the whole range we have to cover
      let fullRange = {
        from: options.getFirstBlockNumber(importState),
        to: item.latestBlockNumber - options.ctx.behavior.waitForBlockPropagation,
      };

      // this can happen when we force the block number in the past and we are treating a recent product
      if (fullRange.from > fullRange.to) {
        if (options.ctx.behavior.forceCurrentBlockNumber !== null) {
          logger.info({
            msg: "current block number set too far in the past to treat this product",
            data: { fullRange, importStateKey: importState.importKey },
          });
        } else {
          logger.error({ msg: "Full range is invalid", data: { fullRange, importStateKey: importState.importKey } });
        }

        // we still need to return something to make the pipeline update the last import date and not loop forever importing the same data again
        return options.formatOutput(item.obj, item.latestBlockNumber, [{ from: fullRange.to, to: fullRange.to }]);
      }

      logger.trace({ msg: "Full range", data: { fullRange, importStateKey: importState.importKey } });

      let ranges = [fullRange];

      // exclude latest block query from the range
      const isLive = options.isLiveItem(item.obj);
      const skipRecent = options.ctx.behavior.skipRecentWindowWhenHistorical;
      let doSkip = false;
      if (skipRecent === "all") {
        doSkip = true;
      } else if (skipRecent === "none") {
        doSkip = false;
      } else if (skipRecent === "live") {
        doSkip = isLive;
      } else if (skipRecent === "eol") {
        doSkip = !isLive;
      } else {
        throw new ProgrammerError({ msg: "Invalid skipRecentWindowWhenHistorical value", data: { skipRecent } });
      }
      if (doSkip) {
        ranges = rangeArrayExclude(ranges, [item.latestBlockQuery]);
      }

      const maxBlocksPerQuery = options.ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
      ranges = _restrictRangesWithImportState(
        options.ctx.behavior,
        ranges,
        importState,
        maxBlocksPerQuery,
        options.ctx.behavior.limitQueriesCountTo.investment,
      );

      // apply forced block number
      if (options.ctx.behavior.forceCurrentBlockNumber !== null) {
        logger.trace({ msg: "Forcing current block number", data: { blockNumber: options.ctx.behavior.forceCurrentBlockNumber } });
        ranges = rangeArrayExclude(ranges, [{ from: options.ctx.behavior.forceCurrentBlockNumber, to: Infinity }]);
      }
      return options.formatOutput(item.obj, item.latestBlockNumber, ranges);
    }),
  );
}

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
      const maxMsPerQuery = samplingPeriodMs[options.ctx.behavior.beefyPriceDataQueryRange];
      const latestDate = new Date();

      // this is the whole range we have to cover
      let fullRange = {
        from: options.getFirstDate(importState),
        to: latestDate,
      };

      let ranges = [fullRange];

      ranges = _restrictRangesWithImportState(
        options.ctx.behavior,
        ranges,
        importState,
        maxMsPerQuery,
        options.ctx.behavior.limitQueriesCountTo.price,
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
      const maxMsPerQuery = samplingPeriodMs[options.ctx.behavior.beefyPriceDataQueryRange];
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

export function addRegularIntervalBlockRangesQueries<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  timeStep: SamplingPeriod;
  getImportState: (item: TObj) => DbBlockNumberRangeImportState;
  chain: Chain;
  formatOutput: (obj: TObj, latestBlockNumber: number, blockRange: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const operator$ = Rx.pipe(
    Rx.tap((_: TObj) => {}),

    Rx.pipe(
      fetchChainBlockList$({
        ctx: options.ctx,
        emitError: options.emitError,
        getChain: () => options.chain,
        timeStep: options.timeStep,
        getFirstDate: (obj) => options.getImportState(obj).importData.contractCreationDate,
        formatOutput: (obj, blockList) => ({ obj, blockList }),
      }),

      // fetch the last block of this chain
      latestBlockNumber$({
        ctx: options.ctx,
        emitError: (item, report) => options.emitError(item.obj, report),
        formatOutput: (item, latestBlockNumber) => ({ ...item, latestBlockNumber }),
      }),
    ),
    Rx.pipe(
      // filter blocks after the creation date of the contract
      Rx.map((item) => ({
        ...item,
        blockList: item.blockList.filter(
          (block) => block.interpolated_block_number >= options.getImportState(item.obj).importData.contractCreatedAtBlock,
        ),
      })),

      // transform to ranges
      Rx.map((item) => {
        const blockRanges: Range<number>[] = [];
        const blockList = sortBy(item.blockList, (block) => block.interpolated_block_number);
        for (let i = 0; i < blockList.length - 1; i++) {
          const block = blockList[i];
          const nextBlock = item.blockList[i + 1];
          blockRanges.push({ from: block.interpolated_block_number, to: nextBlock.interpolated_block_number - 1 });
        }
        return { ...item, blockRanges };
      }),
      // add ranges before and after db blocks
      Rx.map((item) => {
        if (item.blockList.length === 0) {
          return { ...item, blockList: [] };
        }
        const importState = options.getImportState(item.obj);
        const blockNumbers = item.blockList.map((b) => b.interpolated_block_number);
        const minDbBlock = min(blockNumbers) as number;
        const maxDbBlock = max(blockNumbers) as number;

        return {
          ...item,
          blockRanges: [
            { from: importState.importData.contractCreatedAtBlock, to: minDbBlock - 1 },
            ...item.blockRanges,
            { from: maxDbBlock + 1, to: item.latestBlockNumber },
          ],
        };
      }),

      // sometimes the interpolated block numbers are not accurate and the resulting ranges are invalid
      Rx.map((item) => ({ ...item, blockRanges: item.blockRanges.filter((r) => r.from <= r.to) })),
      // filter ranges based on what was already covered
      Rx.map((item) => {
        const importState = options.getImportState(item.obj);
        const maxBlocksPerQuery = options.ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
        const avgMsPerBlock = MS_PER_BLOCK_ESTIMATE[options.chain];
        const maxTimeStepMs = samplingPeriodMs[options.timeStep];
        const avgBlockPerTimeStep = Math.floor(maxTimeStepMs / avgMsPerBlock);
        const rangeMaxLength = Math.min(avgBlockPerTimeStep, maxBlocksPerQuery);
        const ranges = _restrictRangesWithImportState(
          options.ctx.behavior,
          item.blockRanges,
          importState,
          rangeMaxLength,
          options.ctx.behavior.limitQueriesCountTo.shareRate,
        );
        return { ...item, blockRanges: ranges };
      }),
      // transform to query obj
      Rx.map((item) => {
        return {
          input: item.obj,
          output: {
            latestBlockNumber: item.latestBlockNumber,
            blockRanges: item.blockRanges,
          },
        };
      }),
    ),
  );

  return cacheOperatorResult$({
    operator$,
    getCacheKey: (item) => `blockList-${options.chain}-${options.getImportState(item).importKey}-${options.timeStep}`,
    logInfos: { msg: "block list for chain", data: { chain: options.chain } },
    cacheConfig: {
      type: "global",
      stdTTLSec: 5 * 60 /* 5 min */,
      globalKey: "interpolated-product-block-list",
      useClones: false,
    },
    formatOutput: (item, result) => options.formatOutput(item, result.latestBlockNumber, result.blockRanges),
  });
}

export function generateSnapshotQueriesFromEntryAndExits$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  timeStep: SamplingPeriod;
  getImportState: (item: TObj) => DbBlockNumberRangeImportState;
  getEntryAndExitEvents: (item: TObj) => { block_number: number; is_entry: boolean }[];
  formatOutput: (obj: TObj, latestBlockNumber: number, blockRange: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.map((obj) => ({ obj })),
    // get the latest block number

    // first, we need the latest block number
    latestBlockNumber$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      formatOutput: (item, latestBlockNumber) => ({ ...item, latestBlockNumber }),
    }),

    // resolve entry and exit into investment ranges
    Rx.map((item) => {
      const investedRanges: Range<number>[] = [];
      let currentEntry: number | null = null;
      const sortedActions = sortBy(options.getEntryAndExitEvents(item.obj), (action) => action.block_number);

      for (const entryOrExit of sortedActions) {
        if (entryOrExit.is_entry && currentEntry === null) {
          currentEntry = entryOrExit.block_number;
        } else if (!entryOrExit.is_entry && currentEntry !== null) {
          investedRanges.push({ from: currentEntry, to: entryOrExit.block_number });
          currentEntry = null;
        }
        // if we have an exit without an entry, we ignore it
        // if we have an entry without an exit, we ignore it
      }

      // don't forget the latest block number
      if (currentEntry !== null) {
        investedRanges.push({ from: currentEntry, to: item.latestBlockNumber });
      }
      return { ...item, investedRanges };
    }),

    // filter out ranges that are already covered
    Rx.map((item) => {
      const importState = options.getImportState(item.obj);
      const investedRanges = item.investedRanges;

      const maxBlocksPerQuery = options.ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
      const avgMsPerBlock = MS_PER_BLOCK_ESTIMATE[options.ctx.chain];
      const maxTimeStepMs = samplingPeriodMs[options.timeStep];
      const avgBlockPerTimeStep = Math.floor(maxTimeStepMs / avgMsPerBlock);
      const rangeMaxLength = Math.min(avgBlockPerTimeStep, maxBlocksPerQuery);

      const ranges = _restrictRangesWithImportState(
        options.ctx.behavior,
        investedRanges,
        importState,
        rangeMaxLength,
        options.ctx.behavior.limitQueriesCountTo.snapshot,
      );
      return { ...item, investedRanges: ranges };
    }),

    // format output
    Rx.map((item) => options.formatOutput(item.obj, item.latestBlockNumber, item.investedRanges)),
  );
}

export function _restrictRangesWithImportState<T extends SupportedRangeTypes>(
  behavior: ImportBehavior,
  ranges: Range<T>[],
  importState: DbImportState,
  maxRangeLength: number,
  limitRangeCount: number,
): Range<T>[] {
  if (!behavior.ignoreImportState) {
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
