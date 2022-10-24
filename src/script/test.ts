import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { fetchBlockDatetime$ } from "../protocol/common/connector/block-datetime";
import { ImportCtx } from "../protocol/common/types/import-context";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { withPgClient } from "../utils/db";
import { runMain } from "../utils/process";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main(client: PoolClient) {
  const chain: Chain = "moonbeam";
  const ctx: ImportCtx<any> = {
    client,
    emitErrors: (item) => {
      throw new Error("Error for item " + JSON.stringify(item));
    },
    rpcConfig: createRpcConfig(chain),
    streamConfig: {
      maxInputTake: 500,
      maxInputWaitMs: 1000,
      maxTotalRetryMs: 1000,
      workConcurrency: 1,
    },
  };

  // get the block list
  const obs$ = Rx.from([
    1865772, 1865823, 1866600, 1867386, 1868174, 1869449, 1869763, 1868978, 1869781, 1870567, 1871355, 1892897, 1893377, 1893382, 1894241, 1895046,
    1901888, 1902605, 1902850, 1903085, 1903566, 1904306, 1910577, 1911314,
  ]).pipe(
    fetchBlockDatetime$({
      ctx,
      getBlockNumber: (blockNumber) => blockNumber,
      formatOutput: (_, blockList) => {
        return blockList;
      },
    }),
  );
  const res = await consumeObservable(obs$);
  console.dir(res, { depth: null });
}

runMain(withPgClient(main));
