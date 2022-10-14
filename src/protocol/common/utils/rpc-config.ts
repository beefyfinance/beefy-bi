import { Chain } from "../../../types/chain";
import { ethers } from "ethers";
import { sample } from "lodash";
import { RPC_URLS } from "../../../utils/config";
import { addDebugLogsToProvider, monkeyPatchEthersBatchProvider, monkeyPatchHarmonyLinearProvider } from "../../../utils/ethers";
import { getRpcLimitations } from "../../../utils/rpc/rpc-limitations";
import { RpcConfig } from "../../../types/rpc-config";

export function createRpcConfig(chain: Chain): RpcConfig {
  const rpcOptions: ethers.utils.ConnectionInfo = {
    url: sample(RPC_URLS[chain]) as string,
    timeout: 120_000,
  };
  const rpcConfig: RpcConfig = {
    chain,
    linearProvider: new ethers.providers.JsonRpcProvider(rpcOptions),
    batchProvider: new ethers.providers.JsonRpcBatchProvider(rpcOptions),
    limitations: getRpcLimitations(chain, rpcOptions.url),
  };

  addDebugLogsToProvider(rpcConfig.linearProvider);
  addDebugLogsToProvider(rpcConfig.batchProvider);
  monkeyPatchEthersBatchProvider(rpcConfig.batchProvider);
  if (chain === "harmony") {
    monkeyPatchHarmonyLinearProvider(rpcConfig.linearProvider);
  }
  return rpcConfig;
}
