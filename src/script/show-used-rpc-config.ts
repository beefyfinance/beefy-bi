import yargs from "yargs";
import { defaultImportBehavior } from "../protocol/common/types/import-context";
import { createRpcConfig, getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { allChainIds, Chain } from "../types/chain";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { addSecretsToRpcUrl } from "../utils/rpc/remove-secrets-from-rpc-url";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main() {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    chain: {
      choices: allChainIds,
      alias: "c",
      demand: true,
      describe: "show rpc configs for this chain",
    },
    forceRpcUrl: { type: "string", demand: false, alias: "f", describe: "force a specific RPC URL" },
    forceGetLogsBlockSpan: { type: "number", demand: false, alias: "s", describe: "force a specific block span for getLogs" },
    rpcCount: { type: "number", demand: false, alias: "r", describe: "how many RPCs to use" },
    mode: {
      choices: ["historical", "recent"],
      demand: true,
      alias: "t",
      describe: "what mode to use",
    },
  }).argv;

  const options = {
    rpcCount: (argv.rpcCount === undefined || isNaN(argv.rpcCount) ? "all" : argv.rpcCount ?? 0) as number | "all",
    chain: argv.chain as Chain,
    forceRpcUrl: argv.forceRpcUrl ? addSecretsToRpcUrl(argv.forceRpcUrl) : null,
    forceGetLogsBlockSpan: argv.forceGetLogsBlockSpan || null,
    mode: argv.mode as "historical" | "recent",
  };

  const behavior = {
    ...defaultImportBehavior,
    forceRpcUrl: options.forceRpcUrl,
    mode: options.mode,
    forceGetLogsBlockSpan: options.forceGetLogsBlockSpan,
  };

  const rpcConfigs = options.forceRpcUrl
    ? [createRpcConfig(options.chain, behavior)]
    : getMultipleRpcConfigsForChain({
        chain: options.chain,
        behavior,
      });

  console.dir(
    rpcConfigs.map((config) => ({
      rpcUrl: config.linearProvider.connection.url,
      rpcLimitations: config.rpcLimitations,
      hasEtherScanProvider: !!config.etherscan,
    })),
    { depth: null },
  );
}

runMain(main);
