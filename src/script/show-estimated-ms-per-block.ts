import { cloneDeep, sortBy } from "lodash";
import * as Rx from "rxjs";
import yargs from "yargs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { fetchBlockDatetime$ } from "../protocol/common/connector/block-datetime";
import { latestBlockNumber$ } from "../protocol/common/connector/latest-block-number";
import { ImportBehaviour, ImportCtx, createBatchStreamConfig, defaultImportBehaviour } from "../protocol/common/types/import-context";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain, allChainIds } from "../types/chain";
import { DbClient, withDbClient } from "../utils/db";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "script/show-estimated-ms-per-block" });

async function main(client: DbClient) {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    chain: {
      type: "string",
      choices: allChainIds,
      alias: "c",
      demand: true,
      default: "all",
      describe: "only import data for this chain",
    },
  }).argv;

  const options = {
    chain: argv.chain as Chain,
    disableWorkConcurrency: true,
  };
  const behaviour: ImportBehaviour = { ...cloneDeep(defaultImportBehaviour), mode: "recent" };
  const rpcConfig = getMultipleRpcConfigsForChain({ chain: options.chain, behaviour })[0];

  const ctx: ImportCtx = {
    chain: options.chain,
    client,
    behaviour,
    rpcConfig,
    streamConfig: createBatchStreamConfig(options.chain, behaviour),
  };

  const blocksDiff = 10000;
  const pipeline$ = Rx.of(options).pipe(
    latestBlockNumber$({
      ctx,
      emitError: (error, report) => {
        logger.error({ msg: "latestBlockNumber$ error", data: { error, report } });
        throw new Error("latestBlockNumber$ error");
      },
      formatOutput: (item, latestBlockNumber) => ({ ...item, latestBlockNumber }),
    }),
    Rx.concatMap((item) => [item.latestBlockNumber, item.latestBlockNumber - blocksDiff]),
    fetchBlockDatetime$({
      ctx,
      emitError: (error, report) => {
        logger.error({ msg: "fetchBlockDatetime$ error", data: { error, report } });
        throw new Error("fetchBlockDatetime$ error");
      },
      getBlockNumber: (blockNumber) => blockNumber,
      formatOutput: (blockNumber, blockDatetime) => ({ blockNumber, blockDatetime }),
    }),
    Rx.toArray(),
    Rx.map((items) => {
      const sortedBlocks = sortBy(items, "blockNumber");
      const firstBlock = sortedBlocks[0];
      const lastBlock = sortedBlocks[sortedBlocks.length - 1];
      const msDiff = lastBlock.blockDatetime.getTime() - firstBlock.blockDatetime.getTime();
      const msPerBlock = msDiff / blocksDiff;
      return { msPerBlock };
    }),
  );
  const res = await consumeObservable(pipeline$);
  console.dir(res, { depth: 10 });
}

runMain(withDbClient(main, { appName: "show-estimated-ms-per-block", logInfos: { msg: "show-estimated-ms-per-block" } }));
