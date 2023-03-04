import yargs from "yargs";
import { _createImportBehaviorFromCmdParams } from "../protocol/beefy/script/beefy";
import { defaultImportBehavior } from "../protocol/common/types/import-context";
import { createRpcConfig, getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { allChainIds, Chain } from "../types/chain";
import { allSamplingPeriods, SamplingPeriod } from "../types/sampling";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { addSecretsToRpcUrl, removeSecretsFromRpcUrl } from "../utils/rpc/remove-secrets-from-rpc-url";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main() {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    chain: {
      type: "array",
      choices: [...allChainIds, "all"],
      alias: "c",
      demand: false,
      default: "all",
      describe: "only import data for this chain",
    },
    contractAddress: { type: "string", demand: false, alias: "a", describe: "only import data for this contract address" },
    currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "Force the current block number" },
    forceRpcUrl: { type: "string", demand: false, alias: "f", describe: "force a specific RPC URL" },
    forceGetLogsBlockSpan: { type: "number", demand: false, alias: "s", describe: "force a specific block span for getLogs" },
    includeEol: { type: "boolean", demand: false, default: false, alias: "e", describe: "Include EOL products for some chain" },
    task: {
      choices: [
        "historical",
        "recent",
        "products",
        "ignore-address",
        "recent-prices",
        "historical-prices",
        "historical-share-rate",
        "reward-snapshots",
        "investor-cache",
      ],
      demand: true,
      alias: "t",
      describe: "what to run",
    },
    rpcCount: { type: "number", demand: false, alias: "r", describe: "how many RPCs to use" },
    productRefreshInterval: {
      choices: allSamplingPeriods,
      demand: false,
      alias: "p",
      describe: "how often workers should refresh the product list and redispatch accross rpcs",
    },
    loopEvery: { choices: allSamplingPeriods, demand: false, alias: "l", describe: "repeat the task from time to time" },
    ignoreImportState: {
      type: "boolean",
      demand: false,
      default: false,
      alias: "i",
      describe: "ignore the existing import state when generating new queries",
    },
    disableWorkConcurrency: {
      type: "boolean",
      demand: false,
      default: false,
      alias: "C",
      describe: "disable concurrency for work",
    },
    generateQueryCount: {
      type: "number",
      demand: false,
      alias: "q",
      describe: "generate a specific number of queries",
    },
    skipRecentWindowWhenHistorical: {
      choices: ["all", "none", "live", "eol"],
      demand: false,
      default: "all",
      alias: "S",
      describe: "skip the recent window when running historical",
    },
  }).argv;

  const options = {
    rpcCount: (argv.rpcCount === undefined || isNaN(argv.rpcCount) ? "all" : argv.rpcCount ?? 0) as number | "all",
    chain: argv.chain as Chain,
    forceRpcUrl: argv.forceRpcUrl ? addSecretsToRpcUrl(argv.forceRpcUrl) : null,
    forceGetLogsBlockSpan: argv.forceGetLogsBlockSpan || null,
    mode: argv.mode as "historical" | "recent",
  };

  const cmdParams = {
    client: {} as any,
    rpcCount: argv.rpcCount === undefined || isNaN(argv.rpcCount) ? ("all" as const) : argv.rpcCount ?? 0,
    task: argv.task as any,
    includeEol: argv.includeEol,
    filterChains: argv.chain.includes("all") ? allChainIds : ([argv.chain] as Chain[]),
    filterContractAddress: argv.contractAddress || null,
    forceCurrentBlockNumber: argv.currentBlockNumber || null,
    forceRpcUrl: argv.forceRpcUrl ? addSecretsToRpcUrl(argv.forceRpcUrl) : null,
    forceGetLogsBlockSpan: argv.forceGetLogsBlockSpan || null,
    productRefreshInterval: (argv.productRefreshInterval as SamplingPeriod) || null,
    loopEvery: argv.loopEvery || null,
    ignoreImportState: argv.ignoreImportState,
    disableWorkConcurrency: argv.disableWorkConcurrency,
    generateQueryCount: argv.generateQueryCount || null,
    skipRecentWindowWhenHistorical: argv.skipRecentWindowWhenHistorical as any,
  };

  const behavior = _createImportBehaviorFromCmdParams(cmdParams);

  const rpcConfigs = options.forceRpcUrl
    ? [createRpcConfig(options.chain, behavior)]
    : getMultipleRpcConfigsForChain({
        chain: options.chain,
        behavior,
      });

  console.dir(behavior);
  console.dir(
    rpcConfigs.map((config) => ({
      rpcUrl: removeSecretsFromRpcUrl(options.chain, config.linearProvider.connection.url),
      rpcLimitations: config.rpcLimitations,
      hasEtherScanProvider: !!config.etherscan,
    })),
    { depth: null },
  );
}

runMain(main);
