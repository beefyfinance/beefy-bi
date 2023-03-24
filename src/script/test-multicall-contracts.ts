import { ContractCallContext, ContractCallResults, Multicall } from "ethereum-multicall";
import { ethers } from "ethers";
import yargs from "yargs";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { defaultImportBehaviour } from "../protocol/common/types/import-context";
import { createRpcConfig, getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { allChainIds, Chain } from "../types/chain";
import { allSamplingPeriods, SamplingPeriod } from "../types/sampling";
import { BeefyVaultV6AbiInterface, ERC20AbiInterface } from "../utils/abi";
import { getBeefyMulticallAddress } from "../utils/addressbook";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { addSecretsToRpcUrl, removeSecretsFromRpcUrl } from "../utils/rpc/remove-secrets-from-rpc-url";

const logger = rootLogger.child({ module: "show-used-rpc-config", component: "main" });

async function main() {
  const chain = "ethereum" as Chain;
  const cmdParams = {
    client: {} as any,
    rpcCount: "all" as const,
    task: "historical" as const,
    includeEol: true,
    filterChains: [chain],
    filterContractAddress: null,
    forceCurrentBlockNumber: null,
    forceRpcUrl: null,
    forceGetLogsBlockSpan: null,
    productRefreshInterval: null,
    loopEvery: null,
    ignoreImportState: true,
    disableWorkConcurrency: true,
    generateQueryCount: null,
    skipRecentWindowWhenHistorical: "none" as const,
    waitForBlockPropagation: null,
  };

  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);

  const rpcConfigs = getMultipleRpcConfigsForChain({ chain: chain, behaviour });
  const rpcConfig = rpcConfigs[0];
  const provider = rpcConfig.linearProvider;

  const multicallAddress = getBeefyMulticallAddress(chain);

  // you can use any ethers provider context here this example is
  // just shows passing in a default provider, ethers hold providers in
  // other context like wallet, signer etc all can be passed in as well.
  const multicall = new Multicall({ ethersProvider: provider, tryAggregate: true, multicallCustomContractAddress: multicallAddress });

  const vaults = [
    "0x83BE6565c0758f746c09f95559B45Cfb9a0FFFc4",
    "0xCc19786F91BB1F3F3Fd9A2eA9fD9a54F7743039E",
    "0x5Bcd31a28D77a1A5Ef5e0146Ab91e6f43D7100b7",
  ];

  const calls: ContractCallContext[] = [];
  for (const vaultAddress of vaults) {
    const ppfsAbi = {
      inputs: [],
      name: "getPricePerFullShare",
      outputs: [{ internalType: "uint256", name: "", type: "uint256" }],
      stateMutability: "view",
      type: "function",
    };
    const reference = vaultAddress.toLocaleLowerCase();
    calls.push({
      reference: reference,
      contractAddress: vaultAddress,
      abi: [ppfsAbi] as any[],
      calls: [{ reference: reference, methodName: "getPricePerFullShare", methodParameters: [] }],
    });
  }

  const res = await multicall.call(calls, { blockNumber: ethers.utils.hexValue(16896415) });
  console.dir(res);
}

runMain(main);
