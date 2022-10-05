import { ethers } from "ethers";
import { Chain } from "./chain";

export interface RpcConfig {
  chain: Chain;
  // allow users to specify the provider to use
  // most should use the batch provider
  linearProvider: ethers.providers.JsonRpcProvider;
  batchProvider: ethers.providers.JsonRpcBatchProvider;
  maxBatchProviderSize: number;
}
