import { ethers } from "ethers";
import { Chain } from "./chain";

export interface RpcConfig {
  chain: Chain;
  // allow users to specify the provider to use
  // most should use the batch provider
  linearProvider: ethers.providers.JsonRpcProvider;
  batchProvider: ethers.providers.JsonRpcBatchProvider;
  maxBatchProviderSize: {
    [method in RpcCallMethod]: number | null; // null meaning it's not supported
  };
}

export type RpcCallMethod = "eth_getLogs" | "eth_call" | "eth_getBlockByNumber" | "eth_blockNumber";
