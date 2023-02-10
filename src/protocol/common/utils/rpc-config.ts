import { ethers } from "ethers";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { getChainNetworkId } from "../../../utils/addressbook";
import { ETHERSCAN_API_KEY } from "../../../utils/config";

import {
  addDebugLogsToProvider,
  JsonRpcProviderWithMultiAddressGetLogs,
  monkeyPatchAnkrBscLinearProvider,
  monkeyPatchArchiveNodeRpcProvider,
  monkeyPatchCeloProvider,
  monkeyPatchEthersBatchProvider,
  monkeyPatchHarmonyProviderRetryNullResponses,
  monkeyPatchLayer2ReceiptFormat,
  monkeyPatchMissingEffectiveGasPriceReceiptFormat,
  monkeyPatchProviderToRetryUnderlyingNetworkChangedError,
  MultiChainEtherscanProvider,
} from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { removeSecretsFromRpcUrl } from "../../../utils/rpc/remove-secrets-from-rpc-url";
import { getBestRpcUrlsForChain, getRpcLimitations, RpcLimitations } from "../../../utils/rpc/rpc-limitations";

const logger = rootLogger.child({ module: "rpc-utils", component: "rpc-config" });

export function getMultipleRpcConfigsForChain(options: {
  chain: Chain;
  mode: "recent" | "historical";
  rpcCount: number | "all";
  forceGetLogsBlockSpan: number | null;
}): RpcConfig[] {
  let rpcUrls = getBestRpcUrlsForChain(options.chain, options.mode);
  if (options.rpcCount !== "all") {
    rpcUrls = rpcUrls.slice(0, options.rpcCount);
  }
  if (rpcUrls.length === 0) {
    throw new ProgrammerError({ msg: "No matching RPC", data: { chain: options.chain, mode: options.mode, rpcCount: options.rpcCount } });
  }

  logger.debug({ msg: "Using RPC URLs", data: { chain: options.chain, rpcUrls: rpcUrls.map(removeSecretsFromRpcUrl) } });

  return rpcUrls.map((rpcUrl) =>
    createRpcConfig(options.chain, { forceRpcUrl: rpcUrl, mode: options.mode, forceGetLogsBlockSpan: options.forceGetLogsBlockSpan }),
  );
}

const defaultRpcOptions: Partial<ethers.utils.ConnectionInfo> = {
  // disable exponential backoff since we are doing our own retry logic with the callLockProtectedRpc util
  // also, built in exponential retry is very broken and leads to a TimeoutOverflowWarning
  // (node:7615) TimeoutOverflowWarning: 2192352000 does not fit into a 32-bit signed integer.
  // Timeout duration was set to 1.
  throttleLimit: 1,
  throttleCallback: async (attempt: number, url: string) => {
    logger.error({ msg: "RPC call throttled (code 429)", data: { attempt, url } });
    return false;
  },
  allowGzip: true,
  allowInsecureAuthentication: false,
  errorPassThrough: false,
  skipFetchSetup: true,
};

export function createRpcConfig(
  chain: Chain,
  {
    forceRpcUrl,
    mode = "historical",
    timeout = 120_000,
    forceGetLogsBlockSpan = null,
  }: { forceRpcUrl?: string; mode?: "recent" | "historical"; timeout?: number; forceGetLogsBlockSpan?: number | null } = {},
): RpcConfig {
  const rpcUrls = getBestRpcUrlsForChain(chain, mode);
  const rpcUrl = forceRpcUrl || rpcUrls[0];
  logger.info({ msg: "Using RPC", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });

  const rpcOptions: ethers.utils.ConnectionInfo = { ...defaultRpcOptions, url: rpcUrl, timeout };
  const networkish = { name: chain, chainId: getChainNetworkId(chain) };
  const rpcConfig: RpcConfig = {
    chain,
    linearProvider: new JsonRpcProviderWithMultiAddressGetLogs(rpcOptions, networkish),
    batchProvider: new ethers.providers.JsonRpcBatchProvider(rpcOptions, networkish),
    rpcLimitations: getRpcLimitations(chain, rpcOptions.url, forceGetLogsBlockSpan),
  };

  // instantiate etherscan provider
  if (MultiChainEtherscanProvider.isChainSupported(chain)) {
    const apiKey = ETHERSCAN_API_KEY[chain];
    rpcConfig.etherscan = {
      provider: new MultiChainEtherscanProvider(networkish, apiKey || undefined),
      limitations: {
        isArchiveNode: true, // all etherscan providers are archive nodes since they contain all data
        maxGetLogsBlockSpan: 100_000, // unused value
        maxGetLogsAddressBatchSize: null, // unused value
        disableBatching: true, // etherscan doesn't support batching
        internalTimeoutMs: null,
        disableRpc: false,
        weight: null,
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

  monkeyPatchProvider(chain, rpcConfig.linearProvider, rpcConfig.rpcLimitations);
  monkeyPatchProvider(chain, rpcConfig.batchProvider, rpcConfig.rpcLimitations);

  return rpcConfig;
}

export function cloneBatchProvider(chain: Chain, provider: ethers.providers.JsonRpcBatchProvider): ethers.providers.JsonRpcBatchProvider {
  const rpcUrl = provider.connection.url;
  logger.debug({ msg: "Cloning batch RPC", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });

  const rpcOptions: ethers.utils.ConnectionInfo = { ...defaultRpcOptions, url: rpcUrl, timeout: provider.connection.timeout };
  const networkish = { name: chain, chainId: getChainNetworkId(chain) };
  const limitations = getRpcLimitations(chain, rpcOptions.url);

  const batchProvider = new ethers.providers.JsonRpcBatchProvider(rpcOptions, networkish);
  monkeyPatchProvider(chain, batchProvider, limitations);

  return batchProvider;
}

export function monkeyPatchProvider(chain: Chain, provider: ethers.providers.JsonRpcProvider, limitations: RpcLimitations) {
  const isBatchProvider = provider instanceof ethers.providers.JsonRpcBatchProvider;
  const networkish = {
    name: chain,
    chainId: getChainNetworkId(chain),
  };

  // monkey patch providers so they don't call eth_getChainId before every call
  // this effectively divides the number of calls by 2
  // https://github.com/ethers-io/ethers.js/issues/901#issuecomment-647836318
  provider.detectNetwork = () => Promise.resolve(networkish);

  addDebugLogsToProvider(provider);

  if (isBatchProvider) {
    monkeyPatchEthersBatchProvider(provider);
  }

  if (chain === "harmony") {
    monkeyPatchHarmonyProviderRetryNullResponses(provider);
    monkeyPatchMissingEffectiveGasPriceReceiptFormat(provider);
  }
  if (chain === "celo") {
    monkeyPatchCeloProvider(provider);
  }
  if (chain === "optimism" || chain === "metis") {
    monkeyPatchLayer2ReceiptFormat(provider);
  }
  if (chain === "cronos") {
    monkeyPatchMissingEffectiveGasPriceReceiptFormat(provider);
  }

  const retryDelay = limitations.minDelayBetweenCalls === "no-limit" ? 0 : limitations.minDelayBetweenCalls;
  if (limitations.isArchiveNode) {
    monkeyPatchArchiveNodeRpcProvider(provider, retryDelay);
  }

  if (!isBatchProvider && chain === "bsc" && provider.connection.url.includes("ankr")) {
    monkeyPatchAnkrBscLinearProvider(provider, retryDelay);
  }

  monkeyPatchProviderToRetryUnderlyingNetworkChangedError(provider, retryDelay);
}
