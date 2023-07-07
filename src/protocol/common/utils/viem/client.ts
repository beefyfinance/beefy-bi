import { PublicClient, createPublicClient } from "viem";
import { Chain } from "../../../../types/chain";
import { MULTICALL3_ADDRESS_MAP } from "../../../../utils/config";
import { LogInfos } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { removeSecretsFromRpcUrl } from "../../../../utils/rpc/remove-secrets-from-rpc-url";
import { RpcLimitations } from "../../../../utils/rpc/rpc-limitations";
import { BatchStreamConfig, ImportBehaviour } from "../../types/import-context";
import { customHttp } from "./http";

export type CustomViemClient = PublicClient & {
  onError: (handler: (error: unknown) => void) => void;
  clone: () => CustomViemClient;
};

export const createViemPublicClient = ({
  type,
  chain,
  rpcUrl,
  logInfos,
  behaviour,
  limitations,
  streamConfig,
  auth,
}: {
  type: "linear" | "batch";
  chain: Chain;
  rpcUrl: string;
  logInfos: LogInfos;
  behaviour: ImportBehaviour;
  limitations: RpcLimitations;
  streamConfig: BatchStreamConfig;
  auth?: { user: string; password: string };
}): CustomViemClient => {
  const viemHttpTransport = customHttp({ chain, logInfos, limitations, streamConfig }, rpcUrl, {
    batch: limitations.disableBatching || type === "linear" ? undefined : { batchSize: limitations.methods.eth_call || 1000, wait: 100 },
    fetchOptions: auth ? { headers: { Authorization: `Basic ${Buffer.from(`${auth.user}:${auth.password}`).toString("base64")}` } } : undefined,
    key: removeSecretsFromRpcUrl(chain, rpcUrl),
    name: removeSecretsFromRpcUrl(chain, rpcUrl),
    retryCount: 0, // we do our own retry logic
    retryDelay: 0, // we do our own retry logic
    timeout: behaviour.rpcTimeoutMs,
  });

  const client: CustomViemClient = createPublicClient({
    transport: viemHttpTransport,
    batch:
      limitations.disableBatching || type === "linear"
        ? undefined
        : {
            // multicall when available
            multicall: MULTICALL3_ADDRESS_MAP[chain] !== null,
          },
  }) as CustomViemClient;

  // monkey patch the client to add an onError callback
  const defaultErrorHandler = (error: unknown) => {};
  let errorHandler = defaultErrorHandler;

  client.onError = (handler: (error: unknown) => void) => {
    if (errorHandler !== defaultErrorHandler) {
      throw new ProgrammerError("onError can only be set once");
    }
    errorHandler = handler;
  };
  const originalCall = client.call.bind(client);
  const callWithErrorHandler: typeof client.call = async (params) => {
    try {
      return await originalCall(params);
    } catch (error) {
      errorHandler(error);
      throw error;
    }
  };
  client.call = callWithErrorHandler;

  // create a clone method to create a new client with the same config
  // this is useful to make it possible to have multiple parts of the codebase
  // batching calls to the same RPC since batching works per client with viem
  client.clone = () =>
    createViemPublicClient({
      type,
      logInfos,
      chain,
      rpcUrl,
      behaviour,
      limitations,
      streamConfig,
      auth,
    });

  return client;
};
