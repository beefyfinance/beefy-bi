import { ethers } from "ethers";
import { sample } from "lodash";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { RPC_URLS } from "../../../utils/config";
import {
  addDebugLogsToProvider,
  monkeyPatchArchiveNodeRpcProvider,
  monkeyPatchCeloProvider,
  monkeyPatchEthersBatchProvider,
  monkeyPatchHarmonyLinearProvider,
  monkeyPatchHarmonyReceiptFormat,
  monkeyPatchMoonbeamLinearProvider,
  monkeyPatchOptimismReceiptFormat,
  monkeyPatchProviderToRetryUnderlyingNetworkChangedError,
} from "../../../utils/ethers";
import { getRpcLimitations } from "../../../utils/rpc/rpc-limitations";

export function createRpcConfig(chain: Chain, { url: rpcUrl, timeout = 120_000 }: { url?: string; timeout?: number } = {}): RpcConfig {
  const rpcOptions: ethers.utils.ConnectionInfo = {
    url: rpcUrl || (sample(RPC_URLS[chain]) as string),
    timeout,
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
    monkeyPatchHarmonyReceiptFormat(rpcConfig.linearProvider);
    monkeyPatchHarmonyReceiptFormat(rpcConfig.batchProvider);
  }
  if (chain === "moonbeam") {
    monkeyPatchMoonbeamLinearProvider(rpcConfig.linearProvider);
  }
  if (chain === "celo") {
    monkeyPatchCeloProvider(rpcConfig.linearProvider);
    monkeyPatchCeloProvider(rpcConfig.batchProvider);
  }
  if (chain === "optimism") {
    monkeyPatchOptimismReceiptFormat(rpcConfig.linearProvider);
    monkeyPatchOptimismReceiptFormat(rpcConfig.batchProvider);
  }

  const retryDelay = rpcConfig.limitations.minDelayBetweenCalls === "no-limit" ? 0 : rpcConfig.limitations.minDelayBetweenCalls;
  if (rpcConfig.limitations.isArchiveNode) {
    monkeyPatchArchiveNodeRpcProvider(rpcConfig.linearProvider, retryDelay);
    monkeyPatchArchiveNodeRpcProvider(rpcConfig.batchProvider, retryDelay);
  }

  monkeyPatchProviderToRetryUnderlyingNetworkChangedError(rpcConfig.linearProvider, retryDelay);
  monkeyPatchProviderToRetryUnderlyingNetworkChangedError(rpcConfig.batchProvider, retryDelay);

  return rpcConfig;
}
