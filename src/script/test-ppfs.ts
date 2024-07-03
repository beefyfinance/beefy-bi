import { cloneDeep } from "lodash";
import * as Rx from "rxjs";
import { fetchSingleBeefyProductShareRateAndDatetime$ } from "../protocol/beefy/connector/share-rate/share-rate-multi-block";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { createBatchStreamConfig, defaultImportBehaviour, ImportBehaviour, ImportCtx } from "../protocol/common/types/import-context";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main() {
  const options = {
    mode: "historical",
    chain: "linea" as Chain,
    contractAddress: "0x50fa947b08f879004220c42428524eaaf4ef9473",
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
      blockNumber: 6307204,
    },
    {
      vaultDecimals: 18,
      underlyingDecimals: 18,
      vaultAddress: "0x2fcbcf97891c70764c617c41395ccfced3edd06e",
      blockNumber: 6307204,
    },
  ]).pipe(
    Rx.toArray(),
    Rx.concatAll(),
    fetchSingleBeefyProductShareRateAndDatetime$({
      ctx,
      emitError: (obj, err) => {
        console.dir(err, { depth: null });
        throw new Error();
      },
      getCallParams: (item) => ({ type: "fetch", ...item }),
      formatOutput: (obj, ppfs) => ({ obj, ppfs }),
    }),
    Rx.tap((res) =>
      console.dir(
        {
          obj: res.obj,
          ppfs: {
            blockNumber: res.ppfs.blockNumber,
            blockDatetime: res.ppfs.blockDatetime.toISOString(),
            shareRate: res.ppfs.shareRate.toString(),
          },
        },
        { depth: 10 },
      ),
    ),
  );
  await consumeObservable(pipeline$);
}

runMain(main);
