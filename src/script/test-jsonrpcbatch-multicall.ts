import { ethers } from "ethers";
import { Hex } from "../types/address";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../utils/abi";
import { runMain } from "../utils/process";
import { RpcRequestBatch, jsonRpcBatchEthCall } from "../utils/rpc/jsonrpc-batch";

async function main() {
  const chain = "bsc";
  const multicallAddress = "0xcA11bde05977b3631167028862bE2a173976CA11";

  type MulticallCall = {
    allowFailure: boolean;
    callData: string;
    target: Hex;
  };
  const transfers: { blockNumber: number; vaultAddress: Hex; investorAddress: Hex }[] = [
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
  ];

  const jsonRpcBatch: RpcRequestBatch<[MulticallCall[]], { success: boolean; returnData: Hex }[]> = [];
  for (const transferIdx in transfers) {
    const transfer = transfers[transferIdx];

    // create the multicall function call
    const multicallData: MulticallCall[] = [
      {
        allowFailure: true, // can fail when vault is empty
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
    ];

    // create the multicall eth_call call at the right block number
    jsonRpcBatch.push({
      interface: Multicall3AbiInterface,
      contractAddress: multicallAddress,
      params: [multicallData],
      function: "aggregate3",
      blockTag: transfer.blockNumber,
    });
  }

  const result = await jsonRpcBatchEthCall({ url: "https://rpc.ankr.com/bsc", requests: jsonRpcBatch });

  for (const [_, res] of result.entries()) {
    const itemData = await res;
    console.dir({ itemData }, { depth: null });

    const ppfs = BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", itemData[0].returnData)[0] as ethers.BigNumber;
    const balance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", itemData[1].returnData)[0] as ethers.BigNumber;
    const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult("getCurrentBlockTimestamp", itemData[2].returnData)[0] as ethers.BigNumber;

    console.log({ ppfs: ppfs.toString(), balance: balance.toString(), blockTimestamp: new Date(blockTimestamp.toNumber() * 1000) });
  }
}

runMain(main);
