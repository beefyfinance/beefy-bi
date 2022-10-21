import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { fetchChainBlockList$ } from "../protocol/common/loader/chain-block-list";
import { ImportCtx } from "../protocol/common/types/import-context";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { withPgClient } from "../utils/db";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main(client: PoolClient) {
  const chain: Chain = "bsc";
  const ctx: ImportCtx<any> = {
    client,
    emitErrors: (item) => {
      throw new Error("Error for item " + JSON.stringify(item));
    },
    rpcConfig: createRpcConfig(chain),
    streamConfig: {
      maxInputTake: 10,
      maxInputWaitMs: 1000,
      maxTotalRetryMs: 1000,
      workConcurrency: 1,
    },
  };

  // get the block list
  const obs$ = Rx.from([1]).pipe(
    fetchChainBlockList$({
      ctx,
      getChain: () => chain,
      getFirstDate: () => new Date("2021-01-01"),
      formatOutput: (_, blockList) => {
        return blockList;
      },
    }),
  );
  const res = await consumeObservable(obs$);
  console.dir(res, { depth: null });
}

runMain(withPgClient(main));
