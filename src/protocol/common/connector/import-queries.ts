import { backOff } from "exponential-backoff";
import NodeCache from "node-cache";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { samplingPeriodMs } from "../../../types/sampling";
import {
  BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS,
  CHAIN_RPC_MAX_QUERY_BLOCKS,
  MAX_RANGES_PER_PRODUCT_TO_GENERATE,
  MS_PER_BLOCK_ESTIMATE,
} from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { Range, rangeArrayExclude, rangeSplitManyToMaxLength } from "../../../utils/range";
import { DbBlockNumberRangeImportState, DbDateRangeImportState } from "../loader/import-state";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { getRpcRetryConfig } from "../utils/rpc-retry-config";

const logger = rootLogger.child({ module: "common", component: "latest-block-number" });

const latestBlockCache = new NodeCache({ stdTTL: 60 /* 1min */ });

function latestBlockNumber$<TObj, TRes>(options: {
  rpcConfig: RpcConfig;
  forceCurrentBlockNumber: number | null;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, latestBlockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const retryConfig = getRpcRetryConfig({ maxTotalRetryMs: 5_000, logInfos: { msg: "Fetching block number" } });
  return Rx.mergeMap(async (obj) => {
    if (options.forceCurrentBlockNumber !== null) {
      return options.formatOutput(obj, options.forceCurrentBlockNumber);
    }
    const chain = options.rpcConfig.chain;
    const cacheKey = chain;
    let latestBlockNumber = latestBlockCache.get<number>(cacheKey);
    if (latestBlockNumber === undefined) {
      logger.trace({ msg: "Latest block number not found in cache, fetching", data: { chain } });
      latestBlockNumber = await backOff(() => options.rpcConfig.linearProvider.getBlockNumber(), retryConfig);
      latestBlockCache.set(cacheKey, latestBlockNumber);
    }
    return options.formatOutput(obj, latestBlockNumber);
  }, options.streamConfig.workConcurrency);
}

/**
 * Generate a query based on the block
 * used to get last data for the given chain
 */
export function addLatestBlockQuery$<TObj, TRes>(options: {
  rpcConfig: RpcConfig;
  forceCurrentBlockNumber: number | null;
  getLastImportedBlock: (chain: Chain) => number | null;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, latestBlockNumber: number, latestBlockQuery: Range<number>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(options.streamConfig.maxInputWaitMs, undefined, options.streamConfig.maxInputTake),

    // go get the latest block number for this chain
    latestBlockNumber$({
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
  streamConfig: BatchStreamConfig;
  getImport: (obj: TObj) => TImport;
  getFirstBlockNumber: (importState: TImport) => number;
  formatOutput: (obj: TObj, latestBlockNumber: number, historicalBlockQueries: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // go get the latest block number for this chain
    latestBlockNumber$({
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
      streamConfig: options.streamConfig,
      rpcConfig: options.rpcConfig,
      formatOutput: (obj, latestBlockNumber) => ({ obj, latestBlockNumber }),
    }),

    // we can now create the historical block query
    Rx.map((item) => {
      const importState = options.getImport(item.obj);
      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.rpcConfig.chain];

      // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
      // where the RPC don't know about the block number he just gave us
      const waitForBlockPropagation = 5;
      // this is the whole range we have to cover
      let fullRange = {
        from: options.getFirstBlockNumber(importState),
        to: item.latestBlockNumber - waitForBlockPropagation,
      };

      let ranges = [fullRange];

      // exclude the ranges we already covered
      ranges = rangeArrayExclude(ranges, importState.importData.ranges.coveredRanges);

      // split in ranges no greater than the maximum allowed
      ranges = rangeSplitManyToMaxLength(ranges, maxBlocksPerQuery);

      // order by newset first since it's more important and more likely to be available via RPC calls
      ranges = ranges.sort((a, b) => b.to - a.to);

      // then add the ranges we had error on at the end
      const rangesToRetry = rangeSplitManyToMaxLength(importState.importData.ranges.toRetry, maxBlocksPerQuery);
      ranges = ranges.concat(rangesToRetry);

      // limit the amount of queries sent
      if (ranges.length > MAX_RANGES_PER_PRODUCT_TO_GENERATE) {
        ranges = ranges.slice(0, MAX_RANGES_PER_PRODUCT_TO_GENERATE);
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

      // exclude the ranges we already covered
      ranges = rangeArrayExclude(ranges, importState.importData.ranges.coveredRanges);

      // split in ranges no greater than the maximum allowed
      ranges = rangeSplitManyToMaxLength(ranges, maxMsPerQuery);

      // order by newset first since it's more important and more likely to be available via RPC calls
      ranges = ranges.sort((a, b) => b.to.getTime() - a.to.getTime());

      // then add the ranges we had error on at the end
      const rangesToRetry = rangeSplitManyToMaxLength(importState.importData.ranges.toRetry, maxMsPerQuery);
      ranges = ranges.concat(rangesToRetry);

      // limit the amount of queries sent
      if (ranges.length > MAX_RANGES_PER_PRODUCT_TO_GENERATE) {
        ranges = ranges.slice(0, MAX_RANGES_PER_PRODUCT_TO_GENERATE);
      }

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
