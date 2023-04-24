import axios from "axios";
import { ethers } from "ethers";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../utils/abi";
import { runMain } from "../utils/process";

async function main() {
  const chain = "bsc";
  const multicallAddress = "0xcA11bde05977b3631167028862bE2a173976CA11";
  type RpcRequest = { jsonrpc: "2.0"; id: string; method: string; params?: any };

  type Hex = `0x${string}`;
  type MulticallCall = {
    allowFailure: boolean;
    callData: string;
    target: Hex;
  };
  const transfers: { blockNumber: bigint; vaultAddress: Hex; investorAddress: Hex }[] = [
    {
      blockNumber: 27544612n,
      vaultAddress: "0xF80d1196119B65aFc91357Daebb02F368F3Bca02",
      investorAddress: "0x5af1b7124a7275fe166cf7e3a4bb7e3dfff7829e",
    },
    {
      blockNumber: 27551526n,
      vaultAddress: "0x6A3B5A4952791F15Da5763745AfEA8366Ad06C7d",
      investorAddress: "0x643e1004dcb9b37d00442438f69d306473fdf58d",
    },
  ];

  const jsonRpcBatch: RpcRequest[] = [];
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
      jsonrpc: "2.0",
      id: transferIdx,
      method: "eth_call",
      params: [
        {
          from: null,
          to: multicallAddress,
          data: Multicall3AbiInterface.encodeFunctionData("aggregate3", [multicallData]),
        },
        ethers.utils.hexValue(transfer.blockNumber),
      ],
    });
  }
  console.dir(jsonRpcBatch, { depth: null });

  const result = await axios.post("https://rpc.ankr.com/bsc", jsonRpcBatch);
  //const results = await (publicClient.transport.url as BaseRpcRequests["request"])(stringify(jsonRpcBatch));
  const batchResult: { id: string; result: Hex }[] = result.data;
  console.dir(batchResult);

  for (const itemResultIdx in batchResult) {
    const itemResult = batchResult[itemResultIdx];
    const itemData = Multicall3AbiInterface.decodeFunctionResult("aggregate3", itemResult.result)[0] as { success: boolean; returnData: Hex }[];
    //console.log(itemData);

    const ppfs = BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", itemData[0].returnData)[0] as ethers.BigNumber;
    const balance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", itemData[1].returnData)[0] as ethers.BigNumber;
    const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult("getCurrentBlockTimestamp", itemData[2].returnData)[0] as ethers.BigNumber;

    console.log({ ppfs: ppfs.toString(), balance: balance.toString(), blockTimestamp: new Date(blockTimestamp.toNumber() * 1000) });
  }
}

runMain(main);
