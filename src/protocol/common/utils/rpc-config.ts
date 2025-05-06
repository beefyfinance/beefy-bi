import { ethers } from "ethers";
import { isEmpty } from "lodash";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { getChainNetworkId } from "../../../utils/addressbook";
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
} from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { removeSecretsFromRpcUrl } from "../../../utils/rpc/remove-secrets-from-rpc-url";
import { getBestRpcUrlsForChain, getRpcLimitations, RpcLimitations } from "../../../utils/rpc/rpc-limitations";
import { ImportBehaviour } from "../types/import-context";

const logger = rootLogger.child({ module: "rpc-utils", component: "rpc-config" });

export function getMultipleRpcConfigsForChain(options: { chain: Chain; behaviour: ImportBehaviour }): RpcConfig[] {
  let rpcUrls = getBestRpcUrlsForChain(options.chain, options.behaviour);
  if (options.behaviour.rpcCount !== "all") {
    rpcUrls = rpcUrls.slice(0, options.behaviour.rpcCount);
  }
  if (rpcUrls.length === 0) {
    throw new ProgrammerError({
      msg: "No matching RPC",
      data: { chain: options.chain, mode: options.behaviour.mode, rpcCount: options.behaviour.rpcCount },
    });
  }

  logger.debug({ msg: "Using RPC URLs", data: { chain: options.chain, rpcUrls: rpcUrls.map((url) => removeSecretsFromRpcUrl(options.chain, url)) } });

  return rpcUrls.map((rpcUrl) => createRpcConfig(options.chain, { ...options.behaviour, forceRpcUrl: rpcUrl }));
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

export function createRpcConfig(chain: Chain, behaviour: ImportBehaviour): RpcConfig {
  const rpcUrls = getBestRpcUrlsForChain(chain, behaviour);
  const urlObj = new URL(behaviour.forceRpcUrl || rpcUrls[0]);

  // extract basic auth if present
  const user = !isEmpty(urlObj.username) ? urlObj.username : undefined;
  const password = !isEmpty(urlObj.password) ? urlObj.password : undefined;
  urlObj.username = "";
  urlObj.password = "";
  const rpcUrl = urlObj.toString();

  logger.info({ msg: "Using RPC", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });

  const rpcOptions: ethers.utils.ConnectionInfo = { ...defaultRpcOptions, url: rpcUrl, user, password, timeout: behaviour.rpcTimeoutMs };
  const networkish = { name: chain, chainId: getChainNetworkId(chain) };
  const rpcConfig: RpcConfig = {
    chain,
    linearProvider: new JsonRpcProviderWithMultiAddressGetLogs(rpcOptions, networkish),
    batchProvider: new ethers.providers.JsonRpcBatchProvider(rpcOptions, networkish),
    rpcLimitations: getRpcLimitations(chain, rpcOptions.url, behaviour),
  };

  monkeyPatchProvider(chain, rpcConfig.linearProvider, rpcConfig.rpcLimitations);
  monkeyPatchProvider(chain, rpcConfig.batchProvider, rpcConfig.rpcLimitations);

  return rpcConfig;
}

export function cloneBatchProvider(
  chain: Chain,
  behaviour: ImportBehaviour,
  provider: ethers.providers.JsonRpcBatchProvider,
): ethers.providers.JsonRpcBatchProvider {
  const rpcUrl = provider.connection.url;
  logger.debug({ msg: "Cloning batch RPC", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });

  const rpcOptions: ethers.utils.ConnectionInfo = { ...defaultRpcOptions, url: rpcUrl, timeout: provider.connection.timeout };
  const networkish = { name: chain, chainId: getChainNetworkId(chain) };
  const limitations = getRpcLimitations(chain, rpcOptions.url, behaviour);

  const batchProvider = new ethers.providers.JsonRpcBatchProvider(rpcOptions, networkish);
  monkeyPatchProvider(chain, batchProvider, limitations);

  // reattach debug listeners used to save rpc errors
  batchProvider.on("debug", (info) => provider.emit("debug", info));
  batchProvider.on("error", (info) => provider.emit("error", info));

  return batchProvider;
}

function monkeyPatchProvider(chain: Chain, provider: ethers.providers.JsonRpcProvider, limitations: RpcLimitations) {
  const isBatchProvider = provider instanceof ethers.providers.JsonRpcBatchProvider;
  const networkish = {
    name: chain,
    chainId: getChainNetworkId(chain),
  };

  // monkey patch providers so they don't call eth_getChainId before every call
  // this effectively divides the number of calls by 2
  // https://github.com/ethers-io/ethers.js/issues/901#issuecomment-647836318
  provider.detectNetwork = () => Promise.resolve(networkish);

  addDebugLogsToProvider(provider, chain);

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

  const retryDelay = limitations.minDelayBetweenCalls === "no-limit" ? 1000 : limitations.minDelayBetweenCalls;
  if (limitations.isArchiveNode) {
    monkeyPatchArchiveNodeRpcProvider(chain, provider, retryDelay);
  }

  if (!isBatchProvider && chain === "bsc" && provider.connection.url.includes("ankr")) {
    monkeyPatchAnkrBscLinearProvider(chain, provider, retryDelay);
  }

  monkeyPatchProviderToRetryUnderlyingNetworkChangedError(chain, provider, retryDelay);
}
