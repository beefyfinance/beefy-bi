import { max, min } from "lodash";
import * as Rx from "rxjs";
import { fetchBeefyPPFS$ } from "../protocol/beefy/connector/ppfs";
import { fetchBlockDatetime$ } from "../protocol/common/connector/block-datetime";
import { ImportCtx } from "../protocol/common/types/import-context";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../utils/config";
import { DbClient, withPgClient } from "../utils/db";
import { runMain } from "../utils/process";
import { rangeArrayExclude, rangeExcludeMany, rangeMerge } from "../utils/range";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main(client: DbClient) {
  const chain: Chain = "emerald";
  const ctx: ImportCtx<any> = {
    chain,
    client,
    emitErrors: (item) => {
      throw new Error("Error for item " + JSON.stringify(item));
    },
    rpcConfig: createRpcConfig(chain),
    streamConfig: {
      maxInputTake: 500,
      maxInputWaitMs: 1000,
      maxTotalRetryMs: 1000,
      dbMaxInputTake: BATCH_DB_INSERT_SIZE,
      dbMaxInputWaitMs: BATCH_MAX_WAIT_MS,
      workConcurrency: 1,
    },
  };
  /*
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
  */

  /*
  const ranges = [
    { from: 15752182, to: 31661214 },
    { from: 31661905, to: 31664305 },
    { from: 31666325, to: 31668725 },
    { from: 31669867, to: 31672267 },
    { from: 31672589, to: 31674989 },
    { from: 31675534, to: 31677934 },
    { from: 31678963, to: 31681363 },
    { from: 31681763, to: 31684163 },
    { from: 31686483, to: 31688883 },
    { from: 31690212, to: 31692612 },
    { from: 31695273, to: 31697673 },
    { from: 31700367, to: 31702767 },
    { from: 31704706, to: 31707106 },
    { from: 31722666, to: 31725066 },
    { from: 31725833, to: 31728233 },
    { from: 31728601, to: 31731001 },
    { from: 31732364, to: 31734764 },
    { from: 31735429, to: 31737829 },
  ];
  const res = rangeExcludeMany({ from: min(ranges.map((r) => r.from))!, to: max(ranges.map((r) => r.to))! }, ranges);
  console.dir(res, { depth: null });
  console.dir(rangeMerge(ranges), { depth: null });*/

  const queries = [
    // "0x350147",
    // "0x350146",
    // "0x350118",
    // "0x3500be",
    // "0x350064",
    // "0x35000a",
    //"0x34ffb0",
    "0x16e678",
    // "0x16e632"
  ];

  const obs$ = Rx.from(queries).pipe(
    fetchBeefyPPFS$({
      ctx,
      getPPFSCallParams: (item) => ({
        blockNumber: parseInt(item, 16),
        underlyingDecimals: 18,
        vaultAddress: "0xb42441990ffb06f155bb5b52577fb137bcb1eb5f",
        vaultDecimals: 18,
      }),
      formatOutput: (_, ppfs) => {
        return ppfs;
      },
    }),
  );
  const res = await consumeObservable(obs$);
  console.dir(res);
}

runMain(withPgClient(main, { appName: "beefy:test_script", readOnly: false, logInfos: { msg: "test" } }));
