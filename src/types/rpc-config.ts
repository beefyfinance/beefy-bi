import { ethers } from "ethers";
import { RpcLimitations } from "../utils/rpc/rpc-limitations";
import { Chain } from "./chain";

export interface RpcConfig {
  chain: Chain;
  // allow users to specify the provider to use
  // most should use the batch provider
  linearProvider: ethers.providers.JsonRpcProvider;
  batchProvider: ethers.providers.JsonRpcBatchProvider;
  rpcLimitations: RpcLimitations;

  etherscan?: {
    provider: ethers.providers.EtherscanProvider;
    limitations: RpcLimitations;
  };
}

export type RpcCallMethod = "eth_getLogs" | "eth_call" | "eth_getBlockByNumber" | "eth_blockNumber" | "eth_getTransactionReceipt";

export const allRpcCallMethods: RpcCallMethod[] = ["eth_getLogs", "eth_call", "eth_getBlockByNumber", "eth_blockNumber", "eth_getTransactionReceipt"];
