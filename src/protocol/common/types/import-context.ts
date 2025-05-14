import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { SamplingPeriod, allSamplingPeriods } from "../../../types/sampling";
import { DbClient } from "../../../utils/db";
import { LogInfos } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range } from "../../../utils/range";

export interface ImportBehaviour {
  // recent import mode is optimized for speed, we strip down everything that is not needed
  // for example we wait less time before sending the next batch of queries to an rpc provider
  // the goal is to import the new blocks as fast as possible
  // historical import mode is optimized for completeness, we do everything we can to make sure we don't miss anything
  // in historical mode, we can wait longer before sending the next batch of queries to an rpc provider
  // the goal is to make sure we don't miss anything
  mode: "historical" | "recent";

  // override the RPC count to use
  // if null, we use every RPC possible
  rpcCount: number | "all";

  // override the default RPC timeout
  rpcTimeoutMs: number;

  // override the RPC url settings
  // if null, we use the default value from the RPC config
  forceRpcUrl: string | null;

  // wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
  // where the RPC don't know about the block number he just gave us
  waitForBlockPropagation: number;

  // override the chain latest block number
  // if null we query the RPC provider to know the latest block
  // this is useful when we want to import a range of blocks that is not the latest blocks
  forceConsideredBlockRange: Range<number> | null;

  // override the considered date range
  // if null, we consider the whole time range
  // this is useful when we want to import a range of dates that is not the whole time range
  forceConsideredDateRange: Range<Date> | null;

  // override the RPC block span for the getLogs call
  // if null, we use the default value from the RPC config
  forceGetLogsBlockSpan: number | null;

  // when using a chain runner, we poll for input change every this long
  // this will stop and recreate the chain runner every time
  // so set it high enough to avoid recreating the chain runner too often
  // but low enough to ensure that products are not stuck for too long on a single rpc provider
  inputPollInterval: SamplingPeriod;

  // when want at least this much time between two import's starts
  // so when the import is done, we find out if the next import is due or not
  // by doing importStartDate - now > repeatAtLeastEvery
  // if null, we don't repeat the import
  repeatAtMostEvery: SamplingPeriod | null;

  // we don't want the import loop to be exactly the same each time
  // one time, we had 15min repeat and the monitoring was also snapshotting every 15min
  // the 2 import got synchronized and the monitoring missed the underlying behaviour
  // our "total work" grew big for 15min, then the import script triggered, which reduced it to almost nothing
  // and then the monitoring snapshot happenned like clockwork, just after the import was done
  // in the end, the monitoring reported a super low "total work" but the reality was that our users might have waited 15min to get their data
  repeatJitter: number;

  // if true, we don't exclude the current successful import from the queries
  // we often to this to re-import a block range that was already successfully imported but is missing some data
  ignoreImportState: boolean;

  // by default, when running in historical mode, we don't include the scope of the recent mode
  // this is because we don't want the historical and recent import to battle for the same data range
  // but sometimes we want to do that, for example when we want to re-import a range of blocks
  // that was already successfully imported but is missing some data
  // in that case, we want to include the recent scope to make the import simpler to reason about
  // all: skip recent window when historical for all products
  // none: don't skip recent window when historical for any product
  // live: skip recent window when historical for live products
  // eol: skip recent window when historical for eol products
  skipRecentWindowWhenHistorical: "all" | "none" | "live" | "eol";

  // when the rpc limitations config is not found, this config tells us what to do
  // if true we use a default config, if false we throw an error
  useDefaultLimitationsIfNotFound: boolean;

  // if true, we don't make concurrent calls to the rpc providers or anything else
  // this is useful for debugging and testing so the logs are easier to read
  disableConcurrency: boolean;

  // when a product is marked as EOL, wait this long before removing it from the dashboard and stopping rpc imports
  productIsDashboardEolAfter: SamplingPeriod;

  // db batch configs
  // we wait until we either have enough items in the input queue or we have waited long enough
  dbBatch: {
    // how many items to take from the input queue before inserting them into the db
    maxInputTake: number;
    // how long to wait before inserting the items into the db
    maxInputWaitMs: number;
  };

  // memory management configs
  // Since there is no backpressure system in rxjs, we need to limit the number of incoming items
  // Fetching investments is a large operation, so we need to limit the number of concurrent requests
  // but other operations are small, so we can allow more items to be streamed into the input queue
  limitQueriesCountTo: {
    investment: number;
    shareRate: number;
    snapshot: number;
    price: number;
  };

  // how long to wait between each call to the explorers
  minDelayBetweenExplorerCalls: SamplingPeriod;

  // how large of a query range we can make to the beefy api for price data
  beefyPriceDataQueryRange: SamplingPeriod;

  // if we should recalculate price caches after the import
  // this is useful when we want to re-import a range of price blocks
  refreshPriceCaches: boolean;

  // if we should cache bust the beefy api price data
  beefyPriceDataCacheBusting: boolean;
}

export const defaultImportBehaviour: ImportBehaviour = {
  mode: "recent",
  rpcCount: "all",
  forceRpcUrl: null,
  rpcTimeoutMs: 120_000,
  waitForBlockPropagation: 5,
  forceGetLogsBlockSpan: null,
  inputPollInterval: "4hour",
  repeatAtMostEvery: null,
  repeatJitter: 0.05, // default to 5% jitter
  forceConsideredBlockRange: null,
  forceConsideredDateRange: null,
  ignoreImportState: process.env.BEHAVIOUR_IGNORE_IMPORT_STATE === "true",
  skipRecentWindowWhenHistorical: "live", // by default, live products recent data is done by the recent import
  useDefaultLimitationsIfNotFound: process.env.BEHAVIOUR_USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND === "true",
  disableConcurrency: process.env.BEHAVIOUR_DISABLE_WORK_CONCURRENCY === "true",
  productIsDashboardEolAfter: "1month",
  dbBatch: {
    maxInputTake: process.env.BEHAVIOUR_BATCH_DB_INSERT_SIZE ? parseInt(process.env.BEHAVIOUR_BATCH_DB_INSERT_SIZE, 10) : 5000,
    maxInputWaitMs: process.env.BEHAVIOUR_BATCH_MAX_WAIT_MS ? parseInt(process.env.BEHAVIOUR_BATCH_MAX_WAIT_MS, 10) : 5000,
  },
  limitQueriesCountTo: {
    investment: process.env.BEHAVIOUR_LIMIT_INVESTMENT_QUERIES ? parseInt(process.env.BEHAVIOUR_LIMIT_INVESTMENT_QUERIES, 10) : 100,
    shareRate: process.env.BEHAVIOUR_LIMIT_SHARES_QUERIES ? parseInt(process.env.BEHAVIOUR_LIMIT_SHARES_QUERIES, 10) : 1000,
    snapshot: process.env.BEHAVIOUR_LIMIT_SNAPSHOT_QUERIES ? parseInt(process.env.BEHAVIOUR_LIMIT_SNAPSHOT_QUERIES, 10) : 1000,
    price: process.env.BEHAVIOUR_LIMIT_PRICE_QUERIES ? parseInt(process.env.BEHAVIOUR_LIMIT_PRICE_QUERIES, 10) : 1000,
  },
  minDelayBetweenExplorerCalls: "10sec",
  beefyPriceDataQueryRange:
    process.env.BEHAVIOUR_BEEFY_PRICE_DATA_QUERY_RANGE &&
    allSamplingPeriods.includes(process.env.BEHAVIOUR_BEEFY_PRICE_DATA_QUERY_RANGE as SamplingPeriod)
      ? (process.env.BEHAVIOUR_BEEFY_PRICE_DATA_QUERY_RANGE as SamplingPeriod)
      : "3months",
  refreshPriceCaches: process.env.BEHAVIOUR_REFRESH_PRICE_CACHES === "true",
  beefyPriceDataCacheBusting: process.env.BEHAVIOUR_BEEFY_PRICE_DATA_CACHE_BUSTING === "true",
};

export interface BatchStreamConfig {
  // how many items to take from the input stream before making groups
  maxInputTake: number;
  // how long to wait before making groups
  maxInputWaitMs: number;

  // how many items to take from the input stream before making groups
  dbMaxInputTake: number;
  // how long to wait before making groups
  dbMaxInputWaitMs: number;

  // how many concurrent groups to process at the same time
  workConcurrency: number;

  // how long at most to attempt retries
  maxTotalRetryMs: number;
}

export function createBatchStreamConfig(chain: Chain, behaviour: ImportBehaviour): BatchStreamConfig {
  const defaultHistoricalStreamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: behaviour.disableConcurrency ? 1 : 50,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    dbMaxInputTake: behaviour.dbBatch.maxInputTake,
    dbMaxInputWaitMs: behaviour.dbBatch.maxInputWaitMs,
    // and we can afford longer retries
    maxTotalRetryMs: 30_000,
  };
  const defaultMoonbeamHistoricalStreamConfig: BatchStreamConfig = {
    // since moonbeam is so unreliable but we already have a lot of data, we can afford to do 1 at a time
    workConcurrency: behaviour.disableConcurrency ? 1 : 1,
    // moonbeam can be very unreliable, so we write every single data point to the db asap
    maxInputWaitMs: 1000,
    maxInputTake: 50,
    dbMaxInputTake: 10,
    dbMaxInputWaitMs: 30_000,
    // and we can afford longer retries
    maxTotalRetryMs: 30_000,
  };
  const defaultRecentStreamConfig: BatchStreamConfig = {
    // since we are doing live data on a small amount of queries (one per vault)
    // we can afford some amount of concurrency
    workConcurrency: behaviour.disableConcurrency ? 1 : 100,
    // But we can not afford to wait before processing the next batch
    maxInputWaitMs: 5_000,
    maxInputTake: 500,

    dbMaxInputTake: behaviour.dbBatch.maxInputTake,
    dbMaxInputWaitMs: behaviour.dbBatch.maxInputWaitMs,
    // and we cannot afford too long of a retry per product
    maxTotalRetryMs: 10_000,
  };

  if (behaviour.mode === "historical") {
    if (chain === "moonbeam") {
      return defaultMoonbeamHistoricalStreamConfig;
    }
    return defaultHistoricalStreamConfig;
  } else if (behaviour.mode === "recent") {
    return defaultRecentStreamConfig;
  } else {
    throw new ProgrammerError({ msg: "Invalid mode", data: { mode: behaviour.mode } });
  }
}

export type Throwable = Error | string;

export type ErrorReport = {
  previousError?: ErrorReport;
  error?: Throwable;
  infos: LogInfos;
};

export type ErrorEmitter<T> = (obj: T, report: ErrorReport) => void;

export interface ImportCtx {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  // sometimes we don't need it, but it's simpler to pass it everywhere
  chain: Chain;
  rpcConfig: RpcConfig;
  behaviour: ImportBehaviour;
}
