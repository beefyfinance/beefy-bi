import { ethers } from "ethers";
import yargs from "yargs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { allChainIds } from "../types/chain";
import { Multicall3AbiInterface } from "../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../utils/config";
import { runMain } from "../utils/process";

async function main() {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    chain: {
      type: "string",
      choices: allChainIds,
      alias: "c",
      demand: true,
      describe: "chain to test for",
    },
    currentBlockNumber: { type: "number", demand: false, alias: "b", describe: "block number to test" },
    forceRpcUrl: { type: "string", demand: false, alias: "f", describe: "force a specific RPC URL" },
  }).argv;

  const chain = argv.chain;
  let blockNumber = argv.currentBlockNumber;

  const cmdParams = {
    client: {} as any,
    rpcCount: "all" as const,
    task: "historical" as const,
    includeEol: true,
    filterChains: [chain],
    filterContractAddress: null,
    forceConsideredBlockRange: argv.currentBlockNumber ? { from: 0, to: argv.currentBlockNumber } : null,
    forceRpcUrl: argv.forceRpcUrl || null,
    forceGetLogsBlockSpan: null,
    productRefreshInterval: null,
    loopEvery: null,
    loopEveryRandomizeRatio: 0,
    ignoreImportState: true,
    disableWorkConcurrency: true,
    generateQueryCount: null,
    skipRecentWindowWhenHistorical: "none" as const,
    waitForBlockPropagation: null,
  };

  const mcMap = MULTICALL3_ADDRESS_MAP[chain];
  if (!mcMap) {
    throw new Error("No multicall");
  }
  const multicallAddress = mcMap.multicallAddress;

  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);
  const rpcConfigs = getMultipleRpcConfigsForChain({ chain: chain, behaviour });
  const rpcConfig = rpcConfigs[0];
  const provider = rpcConfig.batchProvider;

  const latestBlockNumber = await provider.getBlockNumber();
  if (!blockNumber) {
    blockNumber = Math.floor(latestBlockNumber * 0.99);
  }

  const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);
  const mcRes = await multicallContract.callStatic.aggregate3(
    [
      {
        allowFailure: false,
        callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
        target: multicallAddress,
      },
    ],
    { blockTag: blockNumber },
  );

  const mcBlockTimestamp = Multicall3AbiInterface.decodeFunctionResult("getCurrentBlockTimestamp", mcRes[0].returnData)[0] as ethers.BigNumber;
  const mcBlockDate = new Date(mcBlockTimestamp.toNumber() * 1000);

  const rpcBlock = await provider.getBlock(blockNumber);
  const rpcBlockTimestamp = rpcBlock.timestamp;
  const rpcBlockDate = new Date(rpcBlockTimestamp * 1000);

  console.log({
    chain,
    latestBlockNumber,
    blockNumber,
    mcBlockDate: mcBlockDate.toISOString(),
    rpcBlockDate: rpcBlockDate.toISOString(),
    same: rpcBlockDate.getTime() === mcBlockDate.getTime(),
  });
}

runMain(main);
