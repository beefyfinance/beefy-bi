import { ethers } from "ethers";
import { max, min, uniq } from "lodash";
import * as Rx from "rxjs";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { fetchBeefyPPFS$ } from "../protocol/beefy/connector/ppfs";
import { fetchBlockDatetime$ } from "../protocol/common/connector/block-datetime";
import { fetchERC20TransferEventsFromExplorer } from "../protocol/common/connector/erc20-transfers";
import { ImportCtx } from "../protocol/common/types/import-context";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { sleep } from "../utils/async";
import { BATCH_DB_INSERT_SIZE, BATCH_MAX_WAIT_MS } from "../utils/config";
import { DbClient, withPgClient } from "../utils/db";
import { addDebugLogsToProvider, MultiChainEtherscanProvider } from "../utils/ethers";
import { runMain } from "../utils/process";
import { rangeArrayExclude, rangeExcludeMany, rangeMerge } from "../utils/range";
import { consumeObservable } from "../utils/rxjs/utils/consume-observable";

async function main(client: DbClient) {
  const chain: Chain = "metis";
  const ctx = {
    chain,
    client,
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

  const res = await ctx.rpcConfig.linearProvider.getTransactionReceipt("0x4154d683c5d964963fca3f6d065b501c70bafbb06df009109186fbb6014fe310");
  console.log(res);
  if (1 == 1) return;
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
    "0x350147",
    "0x350146",
    "0x350118",
    "0x3500be",
    "0x350064",
    "0x35000a",
    "0x34ffb0",
    //"0x16e678",
    // "0x16e632"
  ];
  /*
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
  return;*/

  if (!ctx.rpcConfig.etherscan) {
    return;
  }
  const provider = ctx.rpcConfig.etherscan.provider;
  const limitations = ctx.rpcConfig.etherscan.limitations;
  const address = "0x7828ff4ABA7aAb932D8407C78324B069D24284c9";
  const events = await fetchERC20TransferEventsFromExplorer(provider, limitations, "bsc", {
    address,
    decimals: 18,
    fromBlock: 10_000_000,
    toBlock: 22_743_966,
  });
  const eventBlocks = uniq(events.map((e) => e.blockNumber));
  console.dir({ blockmin: min(eventBlocks), blockmax: max(eventBlocks), blockCount: eventBlocks.length, eventCount: events.length }, { depth: null });

  return;
  const contract = new ethers.Contract(address, ERC20Abi, provider);

  const eventFilter = contract.filters.Transfer();
  const fromFilter = contract.filters.Transfer(address, null);
  const toFilter = contract.filters.Transfer(null, address);
  console.dir([eventFilter.topics, fromFilter.topics, toFilter.topics], { depth: null });
  await sleep(1000);
  /*const logs = await provider.getLogs({
    address,
    toBlock: 22743966,
    topics: eventFilter.topics,
  });*/
  const logs = await contract.queryFilter(eventFilter, undefined, 22_743_966);
  const logBlocks = logs.map((l) => l.blockNumber);
  console.dir({ min: min(logBlocks), max: max(logBlocks), count: logs.length }, { depth: null });
}

runMain(withPgClient(main, { appName: "beefy:test_script", readOnly: false, logInfos: { msg: "test" } }));
