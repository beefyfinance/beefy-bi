import { cloneDeep } from "lodash";
import * as Rx from "rxjs";
import { fetchBeefyPPFS$ } from "../protocol/beefy/connector/ppfs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { createBatchStreamConfig, defaultImportBehaviour, ImportBehaviour, ImportCtx } from "../protocol/common/types/import-context";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main() {
  const options = {
    mode: "historical",
    chain: "heco" as Chain,
    contractAddress: "0x9be8485ff97257Aea98A3a9FcfFfD9799F76DeeE",
    disableWorkConcurrency: true,
    rpcCount: "all",
    forceRpcUrl: null,
    ignoreImportState: true,
    skipRecentWindowWhenHistorical: "all", // by default, live products recent data is done by the recent import
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

  const pipeline$ = Rx.from([
    {
      vaultDecimals: 18,
      underlyingDecimals: 18,
      vaultAddress: options.contractAddress,
      blockNumber: 25262658,
    },
    {
      vaultDecimals: 18,
      underlyingDecimals: 18,
      vaultAddress: "0xD34A51815892368fE96D9730376b2CEdE99F83D8",
      blockNumber: 25262658,
    },
  ]).pipe(
    Rx.toArray(),
    Rx.concatAll(),
    fetchBeefyPPFS$({
      ctx,
      emitError: (obj, err) => {
        console.dir(err, { depth: null });
        throw new Error();
      },
      getPPFSCallParams: (item) => item,
      formatOutput: (obj, ppfs) => ({ obj, ppfs }),
    }),
    Rx.tap((res) => console.dir({ obj: res.obj, ppfs: res.ppfs.toString() }, { depth: 10 })),
  );
  await consumeObservable(pipeline$);
}

runMain(main);
