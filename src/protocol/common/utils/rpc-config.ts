import { ethers } from "ethers";
import { sample } from "lodash";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { getChainNetworkId } from "../../../utils/addressbook";
import { ETHERSCAN_API_KEY, RPC_URLS } from "../../../utils/config";

import {
  addDebugLogsToProvider,
  monkeyPatchArchiveNodeRpcProvider,
  monkeyPatchCeloProvider,
  monkeyPatchEthersBatchProvider,
  monkeyPatchHarmonyProviderRetryNullResponses,
  monkeyPatchLayer2ReceiptFormat,
  monkeyPatchMissingEffectiveGasPriceReceiptFormat,
  monkeyPatchMoonbeamLinearProvider,
  monkeyPatchProviderToRetryUnderlyingNetworkChangedError,
  MultiChainEtherscanProvider,
} from "../../../utils/ethers";
import { getRpcLimitations } from "../../../utils/rpc/rpc-limitations";

export function createRpcConfig(chain: Chain, { url: rpcUrl, timeout = 120_000 }: { url?: string; timeout?: number } = {}): RpcConfig {
  const rpcOptions: ethers.utils.ConnectionInfo = {
    url: rpcUrl || (sample(RPC_URLS[chain]) as string),
    timeout,
  };
  const networkish = {
    name: chain,
    chainId: getChainNetworkId(chain),
  };

  const rpcConfig: RpcConfig = {
    chain,
    linearProvider: new ethers.providers.JsonRpcProvider(rpcOptions, networkish),
    batchProvider: new ethers.providers.JsonRpcBatchProvider(rpcOptions, networkish),
    rpcLimitations: getRpcLimitations(chain, rpcOptions.url),
  };

  // instanciate etherscan provider
  if (MultiChainEtherscanProvider.isChainSupported(chain)) {
    const apiKey = ETHERSCAN_API_KEY[chain];
    rpcConfig.etherscan = {
      provider: new MultiChainEtherscanProvider(networkish, apiKey || undefined),
      limitations: {
        isArchiveNode: true, // all etherscan providers are archive nodes since they contain all data
        methods: {
          // no batching is supported
          eth_blockNumber: null,
          eth_getBlockByNumber: null,
          eth_getLogs: null,
          eth_call: null,
          eth_getTransactionReceipt: null,
        },
        minDelayBetweenCalls: apiKey ? Math.ceil(1000.0 / 5.0) /* 5 rps with an api key */ : 5000 /* 1 call every 5s without an api key */,
      },
    };
    addDebugLogsToProvider(rpcConfig.etherscan.provider);
  }

  // monkey patch providers so they don't call eth_getChainId before every call
  // this effectively divides the number of calls by 2
  // https://github.com/ethers-io/ethers.js/issues/901#issuecomment-647836318
  rpcConfig.linearProvider.detectNetwork = () => Promise.resolve(networkish);
  rpcConfig.batchProvider.detectNetwork = () => Promise.resolve(networkish);

  addDebugLogsToProvider(rpcConfig.linearProvider);
  addDebugLogsToProvider(rpcConfig.batchProvider);
  monkeyPatchEthersBatchProvider(rpcConfig.batchProvider);

  if (chain === "harmony") {
    monkeyPatchHarmonyProviderRetryNullResponses(rpcConfig.linearProvider);
    monkeyPatchHarmonyProviderRetryNullResponses(rpcConfig.batchProvider);
    monkeyPatchMissingEffectiveGasPriceReceiptFormat(rpcConfig.linearProvider);
    monkeyPatchMissingEffectiveGasPriceReceiptFormat(rpcConfig.batchProvider);
  }
  if (chain === "moonbeam") {
    monkeyPatchMoonbeamLinearProvider(rpcConfig.linearProvider);
  }
  if (chain === "celo") {
    monkeyPatchCeloProvider(rpcConfig.linearProvider);
    monkeyPatchCeloProvider(rpcConfig.batchProvider);
  }
  if (chain === "optimism" || chain === "metis") {
    monkeyPatchLayer2ReceiptFormat(rpcConfig.linearProvider);
    monkeyPatchLayer2ReceiptFormat(rpcConfig.batchProvider);
  }
  if (chain === "cronos") {
    monkeyPatchMissingEffectiveGasPriceReceiptFormat(rpcConfig.linearProvider);
    monkeyPatchMissingEffectiveGasPriceReceiptFormat(rpcConfig.batchProvider);
  }

  const retryDelay = rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? 0 : rpcConfig.rpcLimitations.minDelayBetweenCalls;
  if (rpcConfig.rpcLimitations.isArchiveNode) {
    monkeyPatchArchiveNodeRpcProvider(rpcConfig.linearProvider, retryDelay);
    monkeyPatchArchiveNodeRpcProvider(rpcConfig.batchProvider, retryDelay);
  }

  monkeyPatchProviderToRetryUnderlyingNetworkChangedError(rpcConfig.linearProvider, retryDelay);
  monkeyPatchProviderToRetryUnderlyingNetworkChangedError(rpcConfig.batchProvider, retryDelay);

  return rpcConfig;
}
