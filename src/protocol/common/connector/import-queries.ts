import { max, min, sortBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import {
  BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS,
  LIMIT_INVESTMENT_QUERIES,
  LIMIT_PRICE_QUERIES,
  LIMIT_SHARES_QUERIES,
  LIMIT_SNAPSHOT_QUERIES,
  MS_PER_BLOCK_ESTIMATE,
} from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { Range, rangeArrayExclude, rangeSort, rangeSplitManyToMaxLength, SupportedRangeTypes } from "../../../utils/range";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { fetchChainBlockList$ } from "../loader/chain-block-list";
import { DbBlockNumberRangeImportState, DbDateRangeImportState, DbImportState } from "../loader/import-state";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { latestBlockNumber$ } from "./latest-block-number";

const logger = rootLogger.child({ module: "common", component: "import-queries" });

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestBlockQuery$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  forceCurrentBlockNumber: number | null;
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
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
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

      // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
      // where the RPC don't know about the block number he just gave us
      const waitForBlockPropagation = 5;
      return objGroup.objs.map((obj) =>
        options.formatOutput(obj, objGroup.latestBlockNumber, {
          from: fromBlock - waitForBlockPropagation,
          to: toBlock - waitForBlockPropagation,
        }),
      );
    }, options.ctx.streamConfig.workConcurrency),
  );
}

export function addHistoricalBlockQuery$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TImport extends DbBlockNumberRangeImportState>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  forceCurrentBlockNumber: number | null;
  getImport: (obj: TObj) => TImport;
  getFirstBlockNumber: (importState: TImport) => number;
  formatOutput: (obj: TObj, latestBlockNumber: number, historicalBlockQueries: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // get the recent data query to avoid double fetching it
    addLatestBlockQuery$({
      ctx: options.ctx,
      emitError: options.emitError,
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
      getLastImportedBlock: () => null,
      formatOutput: (obj, latestBlockNumber, latestBlockQuery) => ({ obj, latestBlockNumber, latestBlockQuery }),
    }),

    // we can now create the historical block query
    Rx.map((item) => {
      const importState = options.getImport(item.obj);

      // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
      // where the RPC don't know about the block number he just gave us
      const waitForBlockPropagation = 5;
      // this is the whole range we have to cover
      let fullRange = {
        from: options.getFirstBlockNumber(importState),
        to: item.latestBlockNumber - waitForBlockPropagation,
      };

      // this can happen when we force the block number in the past and we are treating a recent product
      if (fullRange.from > fullRange.to) {
        if (options.forceCurrentBlockNumber !== null) {
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
      ranges = rangeArrayExclude(ranges, [item.latestBlockQuery]);

      const maxBlocksPerQuery = options.ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
      ranges = restrictRangesWithImportState(ranges, importState, maxBlocksPerQuery, LIMIT_INVESTMENT_QUERIES);

      // apply forced block number
      if (options.forceCurrentBlockNumber !== null) {
        logger.trace({ msg: "Forcing current block number", data: { blockNumber: options.forceCurrentBlockNumber } });
        ranges = rangeArrayExclude(ranges, [{ from: options.forceCurrentBlockNumber, to: Infinity }]);
      }
      return options.formatOutput(item.obj, item.latestBlockNumber, ranges);
    }),
  );
}

export function addHistoricalDateQuery$<TObj, TRes, TImport extends DbDateRangeImportState>(options: {
  getImport: (obj: TObj) => TImport;
  getFirstDate: (importState: TImport) => Date;
  formatOutput: (obj: TObj, latestDate: Date, historicalDateQueries: Range<Date>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // we can now create the historical block query
    Rx.map((item) => {
      const importState = options.getImport(item);
      const maxMsPerQuery = BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS;
      const latestDate = new Date();

      // this is the whole range we have to cover
      let fullRange = {
        from: options.getFirstDate(importState),
        to: latestDate,
      };

      let ranges = [fullRange];

      ranges = restrictRangesWithImportState(ranges, importState, maxMsPerQuery, LIMIT_PRICE_QUERIES);
      return options.formatOutput(item, latestDate, ranges);
    }),
  );
}

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestDateQuery$<TObj, TRes>(options: {
  getLastImportedDate: () => Date | null;
  formatOutput: (obj: TObj, latestDate: Date, recentDateQuery: Range<Date>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.map((item) => {
      const latestDate = new Date();
      const maxMsPerQuery = BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS;
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
  forceCurrentBlockNumber: number | null;
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
        forceCurrentBlockNumber: options.forceCurrentBlockNumber,
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
        const ranges = restrictRangesWithImportState(item.blockRanges, importState, rangeMaxLength, LIMIT_SHARES_QUERIES);
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
    getCacheKey: (item) => `blockList-${options.chain}-${options.getImportState(item).importKey}`,
    logInfos: { msg: "block list for chain", data: { chain: options.chain } },
    stdTTLSec: 5 * 60 /* 5 min */,
    formatOutput: (item, result) => options.formatOutput(item, result.latestBlockNumber, result.blockRanges),
  });
}

export function generateSnapshotQueriesFromEntryAndExits$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  forceCurrentBlockNumber: number | null;
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
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
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

      const ranges = restrictRangesWithImportState(investedRanges, importState, rangeMaxLength, LIMIT_SNAPSHOT_QUERIES);
      return { ...item, investedRanges: ranges };
    }),

    // format output
    Rx.map((item) => options.formatOutput(item.obj, item.latestBlockNumber, item.investedRanges)),
  );
}

function restrictRangesWithImportState<T extends SupportedRangeTypes>(
  ranges: Range<T>[],
  importState: DbImportState,
  maxRangeLength: number,
  limitRangeCount: number,
): Range<T>[] {
  // exclude the ranges we already covered
  ranges = rangeArrayExclude(ranges, importState.importData.ranges.coveredRanges as Range<T>[]);

  // split in ranges no greater than the maximum allowed
  ranges = rangeSplitManyToMaxLength(ranges, maxRangeLength);

  // order by new range first since it's more important and more likely to be available via RPC calls
  ranges = rangeSort(ranges).reverse();

  // then add the ranges we had error on at the end
  let rangesToRetry = rangeSplitManyToMaxLength(importState.importData.ranges.toRetry as Range<T>[], maxRangeLength);
  // retry oldest first
  rangesToRetry = rangeSort(rangesToRetry).reverse();

  // put retries last
  ranges = ranges.concat(rangesToRetry);

  // limit the amount of queries sent
  if (ranges.length > limitRangeCount) {
    ranges = ranges.slice(0, limitRangeCount);
  }
  return ranges;
}
