import { ethers } from "ethers";
import { _createImportBehaviourFromCmdParams } from "../protocol/beefy/script/beefy";
import { getMultipleRpcConfigsForChain } from "../protocol/common/utils/rpc-config";
import { Chain } from "../types/chain";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../utils/config";
import { runMain } from "../utils/process";

async function main() {
  const chain: Chain = "bsc";
  const mcMap = MULTICALL3_ADDRESS_MAP[chain];
  if (!mcMap) {
    throw new Error("No multicall");
  }
  const multicallAddress = mcMap.multicallAddress;

  const cmdParams = {
    client: {} as any,
    rpcCount: "all" as const,
    task: "historical" as const,
    includeEol: true,
    filterChains: [chain],
    filterContractAddress: null,
    forceConsideredBlockRange: null,
    forceRpcUrl: null, //"https://rpc.ankr.com/bsc",
    forceGetLogsBlockSpan: null,
    productRefreshInterval: null,
    loopEvery: null,
    loopEveryRandomizeRatio: 0,
    ignoreImportState: true,
    disableWorkConcurrency: true,
    generateQueryCount: null,
    skipRecentWindowWhenHistorical: "none" as const,
    waitForBlockPropagation: null,
    forceConsideredDateRange: null,
    refreshPriceCaches: false,
    beefyPriceDataQueryRange: null,
    beefyPriceDataCacheBusting: false,
  };

  const behaviour = _createImportBehaviourFromCmdParams(cmdParams);
  const rpcConfigs = getMultipleRpcConfigsForChain({ chain: chain, behaviour });
  const rpcConfig = rpcConfigs[0];
  const provider = rpcConfig.batchProvider;

  const transfers: { blockNumber: number; vaultAddress: string; investorAddress: string }[] = [
    {
      blockNumber: 27544612,
      vaultAddress: "0xF80d1196119B65aFc91357Daebb02F368F3Bca02",
      investorAddress: "0x5af1b7124a7275fe166cf7e3a4bb7e3dfff7829e",
    },
    {
      blockNumber: 27551526,
      vaultAddress: "0x6A3B5A4952791F15Da5763745AfEA8366Ad06C7d",
      investorAddress: "0x643e1004dcb9b37d00442438f69d306473fdf58d",
    },
    // last withdraw from a now empty vault
    {
      blockNumber: 23956036,
      vaultAddress: "0x213526BEF0fA40c7FEAB26304c4260c5CF7e841d",
      investorAddress: "0xd927ce147f098ce634301e6c4281541b1939a132",
    },
  ];

  const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);

  const batch = transfers.map((transfer) =>
    multicallContract.callStatic.aggregate3(
      [
        {
          allowFailure: true,
          callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
          target: transfer.vaultAddress,
        },
        {
          allowFailure: false,
          callData: BeefyVaultV6AbiInterface.encodeFunctionData("balanceOf", [transfer.investorAddress]),
          target: transfer.vaultAddress,
        },
        {
          allowFailure: false,
          callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
          target: multicallAddress,
        },
      ],
      { blockTag: transfer.blockNumber },
    ),
  );
  const result: { success: boolean; returnData: string }[][] = await Promise.all(batch);

  transfers.map((transfer, i) => {
    const itemData = result[i];

    const ppfs = BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", itemData[0].returnData)[0] as ethers.BigNumber;
    const balance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", itemData[1].returnData)[0] as ethers.BigNumber;
    const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult("getCurrentBlockTimestamp", itemData[2].returnData)[0] as ethers.BigNumber;

    console.log({ ppfs: ppfs.toString(), balance: balance.toString(), blockTimestamp: new Date(blockTimestamp.toNumber() * 1000) });
  });
}

runMain(main);
