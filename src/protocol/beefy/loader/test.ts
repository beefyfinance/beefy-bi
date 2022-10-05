import { ethers } from "ethers";
import { sample } from "lodash";
import { RPC_URLS } from "../../../utils/config";
import { runMain } from "../../../utils/process";
import { addDebugLogsToProvider } from "../../../utils/ethers";

const chain = "arbitrum";
const provider = new ethers.providers.JsonRpcBatchProvider({
  url: sample(RPC_URLS[chain]) as string,
  // set a low timeout otherwise ethers keeps all call data in memory until the timeout is reached
  timeout: 30_000,
});
addDebugLogsToProvider(provider);

async function main() {
  const blockNumbers = Array.from({ length: 100 }).map(() => provider.getBlockNumber());
  const res = await Promise.all(blockNumbers);
  console.log(res);
}

runMain(main);
