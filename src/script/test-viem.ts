import type { Abi } from "abitype";
import axios from "axios";
import { Hex, MulticallParameters, createPublicClient, decodeFunctionResult, encodeFunctionData, http, toHex } from "viem";
import { bsc } from "viem/chains";
import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import Multicall3Abi from "../../data/interfaces/standard/Multicall3.json";
import { runMain } from "../utils/process";

async function main() {
  const chain = "bsc";
  const publicClient = createPublicClient({
    chain: bsc,
    transport: http(),
  });
  publicClient.readContract;

  const multicallAddress = "0xcA11bde05977b3631167028862bE2a173976CA11";
  type RpcRequest = { jsonrpc: "2.0"; id: string; method: string; params?: any };
  type MulticallCall = {
    allowFailure: boolean;
    callData: ReturnType<typeof encodeFunctionData>;
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

  const multicallContract = { address: multicallAddress, abi: Multicall3Abi as Abi } as const;
  const jsonRpcBatch: RpcRequest[] = [];
  for (const transferIdx in transfers) {
    const transfer = transfers[transferIdx];
    const vaultContract = { address: transfer.vaultAddress, abi: BeefyVaultV6Abi as Abi } as const;
    const contractCalls: MulticallParameters["contracts"] = [
      {
        ...vaultContract,
        functionName: "getPricePerFullShare", // TODO: DEDUP THAT CALL
      },
      {
        ...vaultContract,
        functionName: "balanceOf",
        args: [transfer.investorAddress],
      },
      {
        ...multicallContract,
        functionName: "getCurrentBlockTimestamp", // TODO: DEDUP THAT CALL
      },
    ];

    // create the multicall function call
    const multicallData = contractCalls.map(
      (functionCall): MulticallCall => ({
        allowFailure: true,
        callData: encodeFunctionData(functionCall),
        target: functionCall.address,
      }),
    );

    // create the multicall eth_call call at the right block number
    jsonRpcBatch.push({
      jsonrpc: "2.0",
      id: transferIdx,
      method: "eth_call",
      params: [
        {
          from: null,
          to: multicallAddress,
          data: encodeFunctionData({
            ...multicallContract,
            functionName: "aggregate3",
            args: [multicallData],
          }),
        },
        toHex(transfer.blockNumber),
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
    const itemData = decodeFunctionResult({
      ...multicallContract,
      functionName: "aggregate3",
      data: itemResult.result,
    }) as { success: boolean; returnData: Hex }[];
    console.log(itemData);

    const transfer = transfers[itemResultIdx];
    const vaultContract = { address: transfer.vaultAddress, abi: BeefyVaultV6Abi as Abi } as const;
    const ppfs = decodeFunctionResult({
      ...vaultContract,
      functionName: "getPricePerFullShare",
      data: itemData[0].returnData,
    });
    const balance = decodeFunctionResult({
      ...vaultContract,
      functionName: "balanceOf",
      data: itemData[1].returnData,
    });
    const blockTimestamp = decodeFunctionResult({
      ...multicallContract,
      functionName: "getCurrentBlockTimestamp",
      data: itemData[2].returnData,
    });

    console.log({ ppfs, balance, blockTimestamp });
  }
}

runMain(main);
