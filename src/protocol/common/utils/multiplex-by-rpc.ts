import { random } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { rangeMerge, rangeOverlap } from "../../../utils/range";
import { removeSecretsFromRpcUrl } from "../../../utils/rpc/remove-secrets-from-rpc-url";
import { ImportCtx } from "../types/import-context";
import { BatchStreamConfig } from "./batch-rpc-calls";
import { createRpcConfig, getMultipleRpcConfigsForChain } from "./rpc-config";

const logger = rootLogger.child({ module: "utils", component: "multiplex-by-rpc" });

export const defaultHistoricalStreamConfig: BatchStreamConfig = {
  // since we are doing many historical queries at once, we cannot afford to do many at once
  workConcurrency: 1,
  // But we can afford to wait a bit longer before processing the next batch to be more efficient
  maxInputWaitMs: 30 * 1000,
  maxInputTake: 500,
  dbMaxInputTake: BATCH_DB_INSERT_SIZE,
  dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
  // and we can afford longer retries
  maxTotalRetryMs: 30_000,
};
export const defaultMoonbeamHistoricalStreamConfig: BatchStreamConfig = {
  // since moonbeam is so unreliable but we already have a lot of data, we can afford to do 1 at a time
  workConcurrency: 1,
  maxInputWaitMs: 1000,
  maxInputTake: 1,
  dbMaxInputTake: 1,
  dbMaxInputWaitMs: 1,
  // and we can afford longer retries
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
  forceRpcUrl: string | null;
  forceGetLogsBlockSpan: number | null;
  mode: "recent" | "historical";
  createPipeline: (ctx: ImportCtx) => Rx.OperatorFunction<TInput, TRes>;
}): Rx.OperatorFunction<TInput, TRes> {
  const rpcConfigs = options.forceRpcUrl
    ? [createRpcConfig(options.chain, { forceRpcUrl: options.forceRpcUrl, mode: options.mode, forceGetLogsBlockSpan: options.forceGetLogsBlockSpan })]
    : getMultipleRpcConfigsForChain({
        chain: options.chain,
        mode: options.mode,
        rpcCount: options.rpcCount,
        forceGetLogsBlockSpan: options.forceGetLogsBlockSpan,
      });
  if (rpcConfigs.length <= 0) {
    throw new ProgrammerError({ msg: "No RPC configs found for chain", chain: options.chain });
  }

  const streamConfig =
    options.mode === "recent"
      ? defaultRecentStreamConfig
      : options.chain === "moonbeam"
      ? defaultMoonbeamHistoricalStreamConfig
      : defaultHistoricalStreamConfig;

  return weightedMultiplex(
    rpcConfigs.map((rpcConfig) => {
      const ctx = { chain: options.chain, client: options.client, rpcConfig, streamConfig };
      return {
        weight:
          ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit"
            ? 1_000_000
            : Math.round(1_000_000 / Math.max(ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls, 500)),
        pipeline: options.createPipeline(ctx),
      };
    }),
  );
}

export function weightedMultiplex<TInput, TRes>(
  branches: { pipeline: Rx.OperatorFunction<TInput, TRes>; weight: number }[],
  rng: typeof random = random,
): Rx.OperatorFunction<TInput, TRes> {
  for (const branch of branches) {
    if (branch.weight < 1) {
      throw new ProgrammerError({ msg: "Branch weight must be positive", branch });
    }
  }

  const totalWeight = branches.reduce((acc, p) => acc + p.weight, 0);

  const minMax: [number, number][] = [];
  let min = 1;
  for (let idx = 0; idx < branches.length; idx++) {
    const p = branches[idx];
    minMax.push([min, min + p.weight - 1]);
    min += p.weight;
  }
  const pipelines = branches.map((p, idx) => ({ ...p, minMax: minMax[idx] }));

  // test if our minMax is correct
  const ranges = pipelines.map((p) => ({ from: p.minMax[0], to: p.minMax[1] }));
  const hasOverlap = ranges.some((r1) => ranges.some((r2) => r1 !== r2 && rangeOverlap(r1, r2)));
  if (hasOverlap) {
    throw new ProgrammerError({ msg: "Branches have overlapping ranges", ranges });
  }
  const isContiguous = rangeMerge(ranges).length === 1;
  if (!isContiguous) {
    throw new ProgrammerError({ msg: "Branches are not contiguous", ranges });
  }
  const isCovering = Math.min(...ranges.map((r) => r.from)) === 1 && Math.max(...ranges.map((r) => r.to)) === totalWeight;
  if (!isCovering) {
    throw new ProgrammerError({ msg: "Branches are not covering", ranges, totalWeight });
  }

  return Rx.pipe(
    // add a unique id to each item
    Rx.map((obj: TInput) => ({ obj, rng: rng(1, totalWeight, false) })),

    Rx.connect((item$) =>
      Rx.merge(
        ...pipelines.map(({ pipeline, minMax }) =>
          item$.pipe(
            Rx.filter((item) => item.rng >= minMax[0] && item.rng <= minMax[1]),
            Rx.map((item) => item.obj),
            pipeline,
          ),
        ),
      ),
    ),
  );
}
