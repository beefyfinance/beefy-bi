import { max, min, sortBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { SamplingPeriod, samplingPeriodMs } from "../../../types/sampling";
import {
  BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS,
  CHAIN_RPC_MAX_QUERY_BLOCKS,
  MAX_RANGES_PER_PRODUCT_TO_GENERATE,
  MS_PER_BLOCK_ESTIMATE,
} from "../../../utils/config";
import { DbClient, db_query_one } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { Range, rangeArrayExclude, rangeSort, rangeSplitManyToMaxLength, SupportedRangeTypes } from "../../../utils/range";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { fetchChainBlockList$ } from "../loader/chain-block-list";
import { DbBlockNumberRangeImportState, DbDateRangeImportState, DbImportState } from "../loader/import-state";
import { ImportCtx } from "../types/import-context";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "common", component: "import-queries" });

export function latestBlockNumber$<TObj, TRes>(options: {
  client: DbClient;
  rpcConfig: RpcConfig;
  forceCurrentBlockNumber: number | null;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, latestBlockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return cacheOperatorResult$({
    stdTTLSec: 60 /* 1min */,
    getCacheKey: () => options.rpcConfig.chain,
    logInfos: { msg: "latest block number", data: { chain: options.rpcConfig.chain } },
    operator$: Rx.mergeMap(async (obj) => {
      if (options.forceCurrentBlockNumber) {
        logger.info({ msg: "Using forced block number", data: { blockNumber: options.forceCurrentBlockNumber, chain: options.rpcConfig.chain } });
        return { input: obj, output: options.forceCurrentBlockNumber };
      }

      try {
        const latestBlockNumber = await callLockProtectedRpc(() => options.rpcConfig.linearProvider.getBlockNumber(), {
          chain: options.rpcConfig.chain,
          provider: options.rpcConfig.linearProvider,
          rpcLimitations: options.rpcConfig.limitations,
          logInfos: { msg: "latest block number", data: { chain: options.rpcConfig.chain } },
          maxTotalRetryMs: options.streamConfig.maxTotalRetryMs,
        });
        return { input: obj, output: latestBlockNumber };
      } catch (err) {
        logger.error({ msg: "Error while fetching latest block number", data: { chain: options.rpcConfig.chain, err } });
      }
      logger.info({ msg: "Using last block number from database", data: { chain: options.rpcConfig.chain } });

      const dbRes = await db_query_one<{ latest_block_number: number }>(
        `
        select last(block_number, datetime) as latest_block_number 
        from block_ts 
        where chain = %L
      `,
        [options.rpcConfig.chain],
        options.client,
      );
      if (!dbRes) {
        throw new Error(`No block number found for chain ${options.rpcConfig.chain}`);
      }
      return { input: obj, output: dbRes.latest_block_number };
    }, options.streamConfig.workConcurrency),
    formatOutput: options.formatOutput,
  });
}

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestBlockQuery$<TObj, TRes>(options: {
  client: DbClient;
  rpcConfig: RpcConfig;
  forceCurrentBlockNumber: number | null;
  getLastImportedBlock: (chain: Chain) => number | null;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, latestBlockNumber: number, latestBlockQuery: Range<number>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(options.streamConfig.maxInputWaitMs, undefined, options.streamConfig.maxInputTake),
    Rx.filter((items) => items.length > 0),

    // go get the latest block number for this chain
    latestBlockNumber$({
      client: options.client,
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
      rpcConfig: options.rpcConfig,
      streamConfig: options.streamConfig,
      formatOutput: (objs, latestBlockNumber) => ({ objs, latestBlockNumber }),
    }),

    // compute the block range we want to query
    Rx.mergeMap((objGroup) => {
      // fetch the last hour of data
      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.rpcConfig.chain];
      const period = samplingPeriodMs["1hour"];
      const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[options.rpcConfig.chain]);

      const lastImportedBlockNumber = options.getLastImportedBlock(options.rpcConfig.chain);
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
    }, options.streamConfig.workConcurrency),
  );
}

export function addHistoricalBlockQuery$<TObj, TRes, TImport extends DbBlockNumberRangeImportState>(options: {
  forceCurrentBlockNumber: number | null;
  rpcConfig: RpcConfig;
  client: DbClient;
  streamConfig: BatchStreamConfig;
  getImport: (obj: TObj) => TImport;
  getFirstBlockNumber: (importState: TImport) => number;
  formatOutput: (obj: TObj, latestBlockNumber: number, historicalBlockQueries: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // go get the latest block number for this chain
    latestBlockNumber$({
      client: options.client,
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
      streamConfig: options.streamConfig,
      rpcConfig: options.rpcConfig,
      formatOutput: (obj, latestBlockNumber) => ({ obj, latestBlockNumber }),
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

      logger.trace({ msg: "Full range", data: { fullRange, importStateKey: importState.importKey } });

      let ranges = [fullRange];

      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.rpcConfig.chain];
      ranges = restrictRangesWithImportState(ranges, importState, maxBlocksPerQuery);

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

      ranges = restrictRangesWithImportState(ranges, importState, maxMsPerQuery);
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

export function addRegularIntervalBlockRangesQueries<TObj, TRes>(options: {
  ctx: ImportCtx<TObj>;
  timeStep: SamplingPeriod;
  getImportState: (item: TObj) => DbBlockNumberRangeImportState;
  forceCurrentBlockNumber: number | null;
  chain: Chain;
  formatOutput: (obj: TObj, latestBlockNumber: number, blockRange: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const operator$ = Rx.pipe(
    Rx.pipe(
      fetchChainBlockList$({
        ctx: options.ctx,
        getChain: () => options.chain,
        timeStep: options.timeStep,
        getFirstDate: (obj) => options.getImportState(obj).importData.contractCreationDate,
        formatOutput: (obj, blockList) => ({ obj, blockList }),
      }),

      // fetch the last block of this chain
      latestBlockNumber$({
        client: options.ctx.client,
        rpcConfig: options.ctx.rpcConfig,
        streamConfig: options.ctx.streamConfig,
        forceCurrentBlockNumber: options.forceCurrentBlockNumber,
        formatOutput: (item, latestBlockNumber) => ({ ...item, latestBlockNumber }),
      }),
    ),
    Rx.pipe(
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
        const blockNumbrers = item.blockList.map((b) => b.interpolated_block_number);
        const minDbBlock = min(blockNumbrers) as number;
        const maxDbBlock = max(blockNumbrers) as number;
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
        const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.chain];
        const avgMsPerBlock = MS_PER_BLOCK_ESTIMATE[options.chain];
        const maxTimeStepMs = samplingPeriodMs[options.timeStep];
        const avgBlockPerTimeStep = Math.floor(maxTimeStepMs / avgMsPerBlock);
        const rangeMaxLength = Math.min(avgBlockPerTimeStep, maxBlocksPerQuery);
        const ranges = restrictRangesWithImportState(item.blockRanges, importState, rangeMaxLength);
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

function restrictRangesWithImportState<T extends SupportedRangeTypes>(
  ranges: Range<T>[],
  importState: DbImportState,
  maxRangeLength: number,
): Range<T>[] {
  // exclude the ranges we already covered
  ranges = rangeArrayExclude(ranges, importState.importData.ranges.coveredRanges as Range<T>[]);

  // split in ranges no greater than the maximum allowed
  ranges = rangeSplitManyToMaxLength(ranges, maxRangeLength);

  // order by newset first since it's more important and more likely to be available via RPC calls
  ranges = rangeSort(ranges).reverse();

  // then add the ranges we had error on at the end
  let rangesToRetry = rangeSplitManyToMaxLength(importState.importData.ranges.toRetry as Range<T>[], maxRangeLength);
  // retry oldest first
  rangesToRetry = rangeSort(rangesToRetry).reverse();

  // put retries last
  ranges = ranges.concat(rangesToRetry);

  // limit the amount of queries sent
  if (ranges.length > MAX_RANGES_PER_PRODUCT_TO_GENERATE) {
    ranges = ranges.slice(0, MAX_RANGES_PER_PRODUCT_TO_GENERATE);
  }
  return ranges;
}
