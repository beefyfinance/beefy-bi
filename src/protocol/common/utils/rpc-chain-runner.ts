import { get, random } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { samplingPeriodMs } from "../../../types/sampling";
import { sleep } from "../../../utils/async";
import { DbClient } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { rangeMerge, rangeOverlap } from "../../../utils/range";
import { removeSecretsFromRpcUrl } from "../../../utils/rpc/remove-secrets-from-rpc-url";
import { consumeStoppableObservable } from "../../../utils/rxjs/utils/consume-observable";
import { saveRpcErrorToDb } from "../connector/rpc-error";
import { createBatchStreamConfig, ImportBehaviour, ImportCtx } from "../types/import-context";
import { createRpcConfig, getMultipleRpcConfigsForChain } from "./rpc-config";

const logger = rootLogger.child({ module: "rpc-utils", component: "rpc-runner" });

export interface ChainRunnerConfig<TInput> {
  chain: Chain;
  client: DbClient;
  getInputs: () => Promise<TInput[]>;
  behaviour: ImportBehaviour;
}

export interface NoRpcRunnerConfig<TInput> {
  client: DbClient;
  getInputs: () => Promise<TInput[]>;
  behaviour: ImportBehaviour;
}

export type RunnerConfig<TInput> = ChainRunnerConfig<TInput> | NoRpcRunnerConfig<TInput>;

function isChainRunnerConfig<TInput>(o: RunnerConfig<TInput>): o is ChainRunnerConfig<TInput> {
  return get(o, ["chain"]) !== undefined;
}

/**
 * - for a single chain, get the corresponding rpcs
 * - for each one, create and start a runner
 * - pull input data from time to time and split them amongst runners
 */
export function createChainRunner<TInput>(
  _options: RunnerConfig<TInput>,
  createPipeline: (ctx: ImportCtx) => Rx.OperatorFunction<TInput, any /* we don't use this result */>,
) {
  let inputs: TInput[] = [];
  let pollerHandle: NodeJS.Timer;

  // we do it that way because it's easy
  const options: ChainRunnerConfig<TInput> = isChainRunnerConfig(_options)
    ? _options
    : {
        ..._options,
        chain: "bsc",
      };

  // get our rpc configs and associated workers
  const rpcConfigs = options.behaviour.forceRpcUrl
    ? [createRpcConfig(options.chain, options.behaviour)]
    : getMultipleRpcConfigsForChain({
        chain: options.chain,
        behaviour: options.behaviour,
      });

  logger.debug({ msg: "splitting inputs between rpcs", data: { rpcCount: rpcConfigs.length } });

  const streamConfig = createBatchStreamConfig(options.chain, options.behaviour);

  const workers = rpcConfigs.map((rpcConfig) => {
    const ctx: ImportCtx = {
      chain: options.chain,
      client: options.client,
      rpcConfig,
      streamConfig,
      behaviour: options.behaviour,
    };
    return {
      runner: createRpcRunner({
        ctx,
        pipeline$: createPipeline(ctx),
      }),
      weight: _getRpcWeight(rpcConfig),
    };
  });

  const updateInputs = async () => {
    // pull some data
    inputs = await options.getInputs();

    // distribute those amongst runners based on their weight
    const inputSplit = _weightedDistribute(inputs, workers);

    // update the runners with the new distribution
    for (const worker of workers) {
      const inputs = inputSplit.get(worker);
      if (!inputs) {
        throw new ProgrammerError("Distribute didn't distribute anything");
      }
      worker.runner.updateInputs(inputs);
    }
  };

  async function run() {
    // get inputs and make sure we poll them at regular interval
    await updateInputs();
    pollerHandle = setInterval(updateInputs, samplingPeriodMs[options.behaviour.inputPollInterval]);

    // start the thingy
    await Promise.all(workers.map((w) => w.runner.run()));
  }

  function stop() {
    clearInterval(pollerHandle);
    for (const worker of workers) {
      worker.runner.stop();
    }
  }

  return { run, stop };
}

export function _getRpcWeight(rpcConfig: RpcConfig): number {
  if (rpcConfig.rpcLimitations.weight !== null) {
    return rpcConfig.rpcLimitations.weight;
  }
  const minDelayBetweenCalls = rpcConfig.rpcLimitations.minDelayBetweenCalls;
  return minDelayBetweenCalls === "no-limit" ? 10_000 : Math.round(1_000_000 / Math.max(minDelayBetweenCalls, 500));
}

export function _weightedDistribute<TInput, TBranch extends { weight: number }>(
  items: TInput[],
  branches: TBranch[],
  rng: typeof random = random,
): Map<TBranch, TInput[]> {
  if (branches.length < 0) {
    return new Map();
  }
  if (branches.length === 1) {
    return new Map([[branches[0], items]]);
  }

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
  const pipelines = branches.map((b, idx) => ({ b, minMax: minMax[idx] }));

  // test if our minMax is correct
  const ranges = pipelines.map((p) => ({ from: p.minMax[0], to: p.minMax[1] }));
  const hasOverlap = ranges.some((r1) => ranges.some((r2) => r1 !== r2 && rangeOverlap(r1, r2)));
  if (hasOverlap) {
    throw new ProgrammerError({ msg: "Branches have overlapping ranges", data: { ranges, pipelines, branches } });
  }
  // test if our minMax has gaps
  const isContiguous = rangeMerge(ranges).length === 1;
  if (!isContiguous) {
    throw new ProgrammerError({ msg: "Branches are not contiguous", data: { ranges, pipelines, branches } });
  }
  // test if our minMax covers the whole range
  const isCovering = Math.min(...ranges.map((r) => r.from)) === 1 && Math.max(...ranges.map((r) => r.to)) === totalWeight;
  if (!isCovering) {
    throw new ProgrammerError({ msg: "Branches are not covering", data: { ranges, totalWeight } });
  }

  const result = new Map<TBranch, TInput[]>();
  for (const branch of branches) {
    result.set(branch, []);
  }

  for (const item of items) {
    const rngValue = rng(1, totalWeight, false);
    const pipeline = pipelines.find((p) => rngValue >= p.minMax[0] && rngValue <= p.minMax[1]);
    if (!pipeline) {
      throw new ProgrammerError({ msg: "No pipeline found for rng value", rngValue, pipelines });
    }
    result.get(pipeline.b)!.push(item);
  }

  return result;
}

/**
 * - apply the pipeline to the input list
 * - When the pipeline ends, restart it with potentially updated input, but respect the `minInterval` param
 */
function createRpcRunner<TInput>(options: { ctx: ImportCtx; pipeline$: Rx.OperatorFunction<TInput, any /* we don't use this result */> }) {
  const logData = {
    chain: options.ctx.rpcConfig.chain,
    rpcUrl: removeSecretsFromRpcUrl(options.ctx.rpcConfig.chain, options.ctx.rpcConfig.linearProvider.connection.url),
  };
  let inputList: TInput[] = [];
  let stop: boolean = false;

  let stopWork: () => void = () => {};

  function updateInputs(inputs: TInput[]) {
    logger.debug({ msg: "Updating inputs ", data: { ...logData, count: inputs.length } });
    stopWork();
    inputList = inputs;
  }

  async function run() {
    while (!stop) {
      if (options.ctx.behaviour.repeatAtMostEvery === null) {
        stop = true;
      }
      logger.debug({ msg: "Starting rpc work unit", data: logData });
      const work = Rx.from(inputList).pipe(options.pipeline$);

      // handle rpc errors
      const errorObsComplete = [
        saveRpcErrorToDb({
          ctx: options.ctx,
          rpc: options.ctx.rpcConfig.linearProvider,
        }),
        saveRpcErrorToDb({
          ctx: options.ctx,
          rpc: options.ctx.rpcConfig.batchProvider,
        }),
      ];

      const start = Date.now();
      const { stop: stopWorker, promise: workerPromise } = consumeStoppableObservable(work);
      stopWork = stopWorker;
      await workerPromise;
      const now = Date.now();

      for (const complete of errorObsComplete) {
        complete();
      }

      logger.debug({ msg: "Done rpc work unit", data: logData });
      if (options.ctx.behaviour.repeatAtMostEvery !== null) {
        const sleepTime = samplingPeriodMs[options.ctx.behaviour.repeatAtMostEvery] - (now - start);
        if (sleepTime > 0) {
          logger.info({ msg: "Sleeping after import", data: { sleepTime } });
          await sleep(sleepTime);
        }
      }
    }
  }

  return {
    updateInputs,
    run,
    stop: () => {
      stop = true;
    },
  };
}
