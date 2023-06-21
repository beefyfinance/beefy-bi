import { ethers } from "ethers";
import { Hex, createPublicClient, getFunctionSelector, http } from "viem";
import { polygon } from "viem/chains";
import { runMain } from "../utils/process";

console.log({ ZE_SELECTOR: getFunctionSelector("typology()") });

const multicallAddress: Hex = "0xcA11bde05977b3631167028862bE2a173976CA11";
const contractCall = {
  abi: [
    {
      inputs: [{ internalType: "bytes32", name: "_priceIdentifier", type: "bytes32" }],
      name: "getLatestPrice",
      outputs: [{ internalType: "uint256", name: "price", type: "uint256" }],
      stateMutability: "view",
      type: "function",
    },
  ],
  address: "0x12F513D977B47D1d155bC5ED4d295c1B10D6D027" as Hex,
  functionName: "getLatestPrice",
  args: ["0x42524c5553440000000000000000000000000000000000000000000000000000" as Hex],
};
const ContractAbi = new ethers.utils.Interface(contractCall.abi);
const Multicall3AbiInterface = new ethers.utils.Interface([
  {
    inputs: [
      {
        components: [
          { internalType: "address", name: "target", type: "address" },
          { internalType: "bool", name: "allowFailure", type: "bool" },
          { internalType: "bytes", name: "callData", type: "bytes" },
        ],
        internalType: "struct Multicall3.Call3[]",
        name: "calls",
        type: "tuple[]",
      },
    ],
    name: "aggregate3",
    outputs: [
      {
        components: [
          { internalType: "bool", name: "success", type: "bool" },
          { internalType: "bytes", name: "returnData", type: "bytes" },
        ],
        internalType: "struct Multicall3.Result[]",
        name: "returnData",
        type: "tuple[]",
      },
    ],
    stateMutability: "payable",
    type: "function",
  },
]);

const viemMulticallHacksoooooor = async () => {
  const client = createPublicClient({ chain: polygon, batch: { multicall: true }, transport: http() });
  const result = await client.readContract({
    abi: [
      {
        inputs: [],
        name: "latestRoundData",
        outputs: [
          { internalType: "uint80", name: "roundId", type: "uint80" },
          { internalType: "int256", name: "answer", type: "int256" },
          { internalType: "uint256", name: "startedAt", type: "uint256" },
          { internalType: "uint256", name: "updatedAt", type: "uint256" },
          { internalType: "uint80", name: "answeredInRound", type: "uint80" },
        ],
        stateMutability: "view",
        type: "function",
      },
    ],
    address: "0xB90DA3ff54C3ED09115abf6FbA0Ff4645586af2c",
    functionName: "latestRoundData",
    args: [] as any,
  });
  console.dir({ THE_RESULT: result }, { depth: null });
};
const viemClientNoBatch = async () => {
  const client = createPublicClient({ chain: polygon, transport: http() });
  const result = await client.readContract({ ...contractCall });
  console.dir({ THE_RESULT: result }, { depth: null });
};

const viemClientNoMulticall = async () => {
  const client = createPublicClient({ chain: polygon, batch: { multicall: false }, transport: http() });
  const result = await client.readContract({ ...contractCall });
  console.dir({ THE_RESULT: result }, { depth: null });
};

const viemClientWithMulticall = async () => {
  const client = createPublicClient({ chain: polygon, batch: { multicall: true }, transport: http() });
  const result = await client.readContract({ ...contractCall });

  console.dir({ THE_RESULT: result }, { depth: null });
};

const ethersClient = async () => {
  const provider = new ethers.providers.JsonRpcProvider("https://rpc.ankr.com/polygon");
  const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);

  const calls = [
    {
      allowFailure: false,
      callData: ContractAbi.encodeFunctionData(contractCall.functionName, contractCall.args),
      target: contractCall.address,
    },
  ];
  const result: { success: boolean; returnData: string }[] = await multicallContract.callStatic.aggregate3(calls);

  console.dir({ THE_MULTICALL_RESULT: result }, { depth: null });
  const callResultData = result[0].returnData;
  const decodedResult = ContractAbi.decodeFunctionResult(contractCall.functionName, callResultData);
  console.dir({ THE_DECODED_RESULT: decodedResult }, { depth: null });
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
runMain(async () => {
  console.log("========================================= viemMulticallHacksoooooor =========================================");
  await viemMulticallHacksoooooor();

  console.log("========================================= viemClientNoBatch =========================================");
  try {
    await viemClientNoBatch();
  } catch (err) {
    console.error(err);
  }

  await sleep(1000);
  console.log("========================================= viemClientNoMulticall =========================================");
  try {
    await viemClientNoMulticall();
  } catch (err) {
    console.error(err);
  }

  await sleep(1000);
  console.log("========================================= viemClientWithMulticall =========================================");
  try {
    await viemClientWithMulticall();
  } catch (err) {
    console.error(err);
  }

  await sleep(1000);
  console.log("========================================= ethersClient =========================================");
  try {
    await ethersClient();
  } catch (err) {
    console.error(err);
  }
});
