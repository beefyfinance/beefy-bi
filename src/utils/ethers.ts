import { RPC_URLS } from "./config";
import { Chain } from "../types/chain";
import * as ethers from "ethers";
import { cacheAsyncResultInRedis } from "./cache";
import * as lodash from "lodash";

function getProvider(chain: Chain) {
  return new ethers.providers.JsonRpcProvider(lodash.sample(RPC_URLS[chain]));
}
/*
export function getContract(
  chain: Chain,
  abi: ethers.ContractInterface,
  address: string
) {
  return new ethers.Contract(address, abi, getProvider(chain));
}*/

export interface BlockDateInfos {
  blockNumber: number;
  datetime: Date;
}

export async function fetchBlockData(
  chain: Chain,
  blockNumber: ethers.ethers.providers.BlockTag
): Promise<BlockDateInfos> {
  const provider = getProvider(chain);
  const block = await provider.getBlock(blockNumber);
  return {
    blockNumber: block.number,
    datetime: new Date(block.timestamp * 1000),
  };
}

export const getRedisCachedBlockDate = cacheAsyncResultInRedis(fetchBlockData, {
  getKey: (chain, blockNumber) => `${chain}:${blockNumber}`,
  dateFields: ["datetime"],
});

export function normalizeAddress(address: string) {
  return ethers.utils.getAddress(address);
}
