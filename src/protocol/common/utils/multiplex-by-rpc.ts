import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { DbClient } from "../../../utils/db";
import { ImportCtx } from "../types/import-context";
import { BatchStreamConfig } from "./batch-rpc-calls";
import { getMultipleRpcConfigsForChain } from "./rpc-config";

export const defaultHistoricalStreamConfig: BatchStreamConfig = {
  // since we are doing many historical queries at once, we cannot afford to do many at once
  workConcurrency: 1,
  // But we can afford to wait a bit longer before processing the next batch to be more efficient
  maxInputWaitMs: 30 * 1000,
  maxInputTake: 500,
  dbMaxInputTake: BATCH_DB_INSERT_SIZE,
  dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
  // and we can affort longer retries
  maxTotalRetryMs: 30_000,
};
export const defaultMoonbeamHistoricalStreamConfig: BatchStreamConfig = {
  // since moonbeam is so unreliable but we already have a lot of data, we can afford to do 1 at a time
  workConcurrency: 1,
  maxInputWaitMs: 1000,
  maxInputTake: 1,
  dbMaxInputTake: 1,
  dbMaxInputWaitMs: 1,
  // and we can affort longer retries
  maxTotalRetryMs: 30_000,
};
export const defaultRecentStreamConfig: BatchStreamConfig = {
  // since we are doing live data on a small amount of queries (one per vault)
  // we can afford some amount of concurrency
  workConcurrency: 10,
  // But we can not afford to wait before processing the next batch
  maxInputWaitMs: 5_000,
  maxInputTake: 500,

  dbMaxInputTake: BATCH_DB_INSERT_SIZE,
  dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
  // and we cannot afford too long of a retry per product
  maxTotalRetryMs: 10_000,
};

export function multiplexByRcp<TInput, TRes>(options: {
  chain: Chain;
  client: DbClient;
  rpcCount: number;
  mode: "recent" | "historical";
  createPipeline: (ctx: ImportCtx) => Rx.OperatorFunction<TInput, TRes>;
}): Rx.OperatorFunction<TInput, TRes> {
  const rpcConfigs = getMultipleRpcConfigsForChain(options.chain, options.mode, options.rpcCount);
  const streamConfig =
    options.mode === "recent"
      ? defaultRecentStreamConfig
      : options.chain === "moonbeam"
      ? defaultMoonbeamHistoricalStreamConfig
      : defaultHistoricalStreamConfig;

  const pipelines = rpcConfigs
    .map((rpcConfig): ImportCtx => ({ chain: options.chain, client: options.client, rpcConfig, streamConfig }))
    .map(options.createPipeline);

  let idx = 1;
  return Rx.pipe(
    // add a unique id to each item
    Rx.map((obj: TInput) => ({ obj, idx: idx++ })),

    Rx.connect((item$) =>
      Rx.merge(
        ...pipelines.map((pipeline) =>
          item$.pipe(
            Rx.filter((item) => item.idx % pipelines.length === pipelines.indexOf(pipeline)),
            Rx.map((item) => item.obj),
            pipeline,
          ),
        ),
      ),
    ),
  );
}
