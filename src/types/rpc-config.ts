import { ethers } from "ethers";
import { BatchStreamConfig } from "../protocol/common/types/import-context";
import { CustomViemClient } from "../protocol/common/utils/viem/client";
import { JsonRpcProviderWithMultiAddressGetLogs } from "../utils/ethers";
import { LogInfos } from "../utils/logger";
import { RpcLimitations } from "../utils/rpc/rpc-limitations";
import { Chain } from "./chain";

export interface RpcConfig {
  chain: Chain;
  // allow users to specify the provider to use
  // most should use the batch provider
  linearProvider: JsonRpcProviderWithMultiAddressGetLogs;
  batchProvider: ethers.providers.JsonRpcBatchProvider;
  getViemClient: (type: "batch" | "linear", logInfos: LogInfos, streamConfig: BatchStreamConfig) => CustomViemClient;
  rpcLimitations: RpcLimitations;
  etherscan?: {
    provider: ethers.providers.EtherscanProvider;
    limitations: RpcLimitations;
  };
}

export type RpcCallMethod = "eth_getLogs" | "eth_call" | "eth_getBlockByNumber" | "eth_blockNumber" | "eth_getTransactionReceipt";

export const allRpcCallMethods: RpcCallMethod[] = ["eth_getLogs", "eth_call", "eth_getBlockByNumber", "eth_blockNumber", "eth_getTransactionReceipt"];
