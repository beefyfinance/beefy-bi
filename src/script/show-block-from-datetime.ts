import { cloneDeep } from "lodash";
import * as Rx from "rxjs";
import yargs from "yargs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { fetchBlockFromDatetime$ } from "../protocol/common/connector/block-from-datetime";
import { ImportBehaviour, ImportCtx, createBatchStreamConfig, defaultImportBehaviour } from "../protocol/common/types/import-context";
import { batchRpcCalls$ } from "../protocol/common/utils/batch-rpc-calls";
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
      describe: "only import data for this chain",
    },
    datetime: {
      type: "string",
      alias: "d",
      demand: true,
      describe: "datetime to find block for (ISO format)",
    },
  }).argv;

  const options = {
    chain: argv.chain as Chain,
    datetime: new Date(argv.datetime),
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

  const pipeline$ = Rx.of(options).pipe(
    fetchBlockFromDatetime$({
      ctx,
      emitError: (error, report) => {
        logger.error({ msg: "fetchBlockFromDatetime$ error", data: { error, report } });
        throw new Error("fetchBlockFromDatetime$ error");
      },
      getBlockDate: (item) => new Date(item.datetime),
      formatOutput: (item, blockNumber) => ({ ...item, blockNumber }),
    }),

    batchRpcCalls$({
      ctx,
      emitError: (error, report) => {
        logger.error({ msg: "batchRpcCalls$ error", data: { error, report } });
        throw new Error("batchRpcCalls$ error");
      },
      getQuery: (item) => item.blockNumber,
      processBatch: async (provider, queryObjs) => {
        const blocks = await Promise.all(queryObjs.map((blockNumber) => provider.getBlock(blockNumber)));
        return {
          successes: new Map(blocks.map((block) => [block.number, block])),
          errors: new Map(),
        };
      },
      rpcCallsPerInputObj: { eth_getBlockByNumber: 1, eth_getLogs: 1, eth_blockNumber: 1, eth_call: 1, eth_getTransactionReceipt: 1 },
      logInfos: { msg: "fetching block" },
      formatOutput: (item, block) => ({ ...item, block }),
    }),

    Rx.map((item) => {
      const block = item.block;
      const blockNumber = block.number;
      const blockTimestamp = block.timestamp;
      const blockDateTime = new Date(blockTimestamp * 1000);
      return { ...item, blockNumber, blockDateTime };
    }),
  );
  const res = await consumeObservable(pipeline$);
  console.dir(res, { depth: 10 });
}

runMain(withDbClient(main, { appName: "show-estimated-ms-per-block", logInfos: { msg: "show-estimated-ms-per-block" } }));
