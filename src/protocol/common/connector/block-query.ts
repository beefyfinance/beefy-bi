import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { samplingPeriodMs } from "../../../types/sampling";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MS_PER_BLOCK_ESTIMATE } from "../../../utils/config";
import NodeCache from "node-cache";
import { Range, rangeArrayExclude, rangeSplitManyToMaxLength } from "../../../utils/range";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { backOff } from "exponential-backoff";
import { getRpcRetryConfig } from "../utils/rpc-retry-config";
import { RpcConfig } from "../../../types/rpc-config";
import { rootLogger } from "../../../utils/logger";
import { DbProductImport } from "../loader/product-import";

const logger = rootLogger.child({ module: "common", component: "latest-block-number" });

const latestBlockCache = new NodeCache({ stdTTL: 60 /* 1min */ });

function latestBlockNumber$<TObj, TRes>(options: {
  getChain: (obj: TObj) => Chain;
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
    const chain = options.getChain(obj);
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
  chain: Chain;
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
      getChain: () => options.chain,
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
      rpcConfig: options.rpcConfig,
      streamConfig: options.streamConfig,
      formatOutput: (objs, latestBlockNumber) => ({ objs, latestBlockNumber }),
    }),

    // compute the block range we want to query
    Rx.mergeMap((objGroup) => {
      // fetch the last hour of data
      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.chain];
      const period = samplingPeriodMs["1hour"];
      const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[options.chain]);

      const lastImportedBlockNumber = options.getLastImportedBlock(options.chain);
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

export function addHistoricalBlockQuery$<TObj, TRes>(options: {
  client: PoolClient;
  chain: Chain;
  forceCurrentBlockNumber: number | null;
  rpcConfig: RpcConfig;
  streamConfig: BatchStreamConfig;
  getProductImport: (obj: TObj) => DbProductImport;
  formatOutput: (obj: TObj, latestBlockNumber: number, historicalBlockQueries: Range<number>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // go get the latest block number for this chain
    latestBlockNumber$({
      getChain: () => options.chain,
      forceCurrentBlockNumber: options.forceCurrentBlockNumber,
      streamConfig: options.streamConfig,
      rpcConfig: options.rpcConfig,
      formatOutput: (obj, latestBlockNumber) => ({ obj, latestBlockNumber }),
    }),

    // we can now create the historical block query
    Rx.map((item) => {
      const productImport = options.getProductImport(item.obj);
      const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[options.chain];

      // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
      // where the RPC don't know about the block number he just gave us
      const waitForBlockPropagation = 5;
      // this is the whole range we have to cover
      let fullRange = {
        from: productImport.importData.contractCreatedAtBlock,
        to: item.latestBlockNumber - waitForBlockPropagation,
      };

      let ranges = [fullRange];

      // exclude the ranges we already covered
      ranges = rangeArrayExclude(ranges, productImport.importData.ranges.coveredRanges);

      // split in ranges no greater than the maximum allowed
      ranges = rangeSplitManyToMaxLength(ranges, maxBlocksPerQuery);

      // order by newset first since it's more important and more likely to be available via RPC calls
      ranges = ranges.sort((a, b) => b.to - a.to);

      // then add the ranges we had error on at the end
      const rangesToRetry = rangeSplitManyToMaxLength(productImport.importData.ranges.toRetry, maxBlocksPerQuery);
      ranges = ranges.concat(rangesToRetry);

      // limit the amount of queries sent
      if (ranges.length > 300) {
        ranges = ranges.slice(0, 300);
      }

      return options.formatOutput(item.obj, item.latestBlockNumber, ranges);
    }),
  );
}
