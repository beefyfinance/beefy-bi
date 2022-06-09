import { RPC_URLS } from "./config";
import { Chain } from "../types/chain";
import * as ethers from "ethers";
import { cacheAsyncResult } from "./cache";

function getProvider(chain: Chain) {
  return new ethers.providers.JsonRpcProvider(RPC_URLS[chain]);
}

export function getContract(
  chain: Chain,
  abi: ethers.ContractInterface,
  address: string
) {
  return new ethers.Contract(address, abi, getProvider(chain));
}

export const getBlockDate = cacheAsyncResult(
  async function fetchBlockData(chain: Chain, blockNumber: number) {
    const provider = getProvider(chain);
    const block = await provider.getBlock(blockNumber);
    return { datetime: new Date(block.timestamp * 1000) };
  },
  {
    getKey: (chain, blockNumber) => `${chain}:${blockNumber}`,
    dateFields: ["datetime"],
  }
);
