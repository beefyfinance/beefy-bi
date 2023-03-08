import { cloneDeep } from "lodash";
import * as Rx from "rxjs";
import yargs from "yargs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { fetchContractCreationInfos$ } from "../protocol/common/connector/contract-creation";
import { createBatchStreamConfig, defaultImportBehaviour, ImportBehaviour, ImportCtx } from "../protocol/common/types/import-context";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { allChainIds, Chain } from "../types/chain";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main() {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    chain: {
      type: "string",
      choices: allChainIds,
      alias: "c",
      demand: true,
      default: "all",
      describe: "only import data for this chain",
    },
    contractAddress: { type: "string", demand: true, alias: "a", describe: "only import data for this contract address" },
    disableWorkConcurrency: {
      type: "boolean",
      demand: false,
      default: false,
      alias: "C",
      describe: "disable concurrency for work",
    },
  }).argv;

  const options = {
    chain: argv.chain as Chain,
    contractAddress: argv.contractAddress,
    disableWorkConcurrency: argv.disableWorkConcurrency,
  };
  const behaviour: ImportBehaviour = { ...cloneDeep(defaultImportBehaviour), mode: "historical" };
  const rpcConfig = getMultipleRpcConfigsForChain({ chain: options.chain, behaviour })[0];

  const ctx: ImportCtx = {
    chain: options.chain,
    client: {} as any, // unused
    behaviour,
    rpcConfig,
    streamConfig: createBatchStreamConfig(options.chain, behaviour),
  };

  const pipeline$ = Rx.of(options).pipe(
    fetchContractCreationInfos$({
      ctx,
      getCallParams: (item) => {
        return {
          chain: item.chain,
          contractAddress: item.contractAddress,
        };
      },
      formatOutput: (contractAddress, contractCreationInfos) => ({ contractAddress, contractCreationInfos }),
    }),
  );
  const res = await consumeObservable(pipeline$);
  console.dir(res, { depth: 10 });
}

runMain(main);
