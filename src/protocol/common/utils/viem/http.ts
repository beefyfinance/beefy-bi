import { RpcRequestError, UrlRequiredError, createTransport, type HttpTransport, type HttpTransportConfig } from "viem";
import { createBatchScheduler } from "viem/src/utils/promise/createBatchScheduler";
import { rpc, type RpcRequest } from "viem/src/utils/rpc.js";
import { Chain } from "../../../../types/chain";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { removeSecretsFromRpcUrl } from "../../../../utils/rpc/remove-secrets-from-rpc-url";
import { RpcLimitations } from "../../../../utils/rpc/rpc-limitations";
import { callLockProtectedRpc } from "../../../../utils/shared-resources/shared-rpc";
import { BatchStreamConfig } from "../../types/import-context";

const logger = rootLogger.child({ module: "utils", component: "viem-http" });

/**
 * Updates viem http transport to add:
 * - ethers like debug hooks
 */
export function customHttp(
  {
    chain: beefyChain,
    logInfos,
    limitations,
    streamConfig,
  }: { chain: Chain; logInfos: LogInfos; limitations: RpcLimitations; streamConfig: BatchStreamConfig },
  /** URL of the JSON-RPC API. Defaults to the chain's public RPC URL. */
  url?: string,
  config: HttpTransportConfig = {},
): HttpTransport {
  const { batch, fetchOptions, key = "http", name = "HTTP JSON-RPC", retryDelay } = config;
  return ({ chain, retryCount: retryCount_, timeout: timeout_ }) => {
    const { batchSize = 1000, wait = 0 } = typeof batch === "object" ? batch : {};
    const retryCount = config.retryCount ?? retryCount_;
    const timeout = timeout_ ?? config.timeout ?? 10_000;
    const url_ = url || chain?.rpcUrls.default.http[0];
    if (!url_) throw new UrlRequiredError();

    // ============================================= CHANGED HERE =============================================
    // changelist:
    // - add logs
    // - add callLockProtectedRpc
    // - add onError callback and set it on the client
    const rpcHttp: typeof rpc.http = async (url, options) => {
      const safeToLogUrl = removeSecretsFromRpcUrl(beefyChain, url);
      const response = await callLockProtectedRpc(
        async () => {
          try {
            logger.trace({ msg: "RPC request", data: { request: options.body, rpcUrl: safeToLogUrl } });
            const res = await rpc.http(url, options);
            logger.trace({ msg: "RPC response", data: { request: options.body, response, rpcUrl: safeToLogUrl } });
            return res;
          } catch (error) {
            logger.trace({ msg: "RPC error", data: { request: options.body, error, rpcUrl: safeToLogUrl } });
            throw error;
          }
        },
        {
          chain: beefyChain,
          rpcLimitations: limitations,
          logInfos: mergeLogsInfos({ msg: "viem client", data: { chain: beefyChain, rpcUrl: safeToLogUrl } }, logInfos),
          maxTotalRetryMs: streamConfig.maxTotalRetryMs,
          noLockIfNoLimit: !!config.batch, // no lock when using batch provider because we are using a copy of the provider
          provider: url,
        },
      );

      return response;
    };
    // ========================================================================================================

    return createTransport(
      {
        key,
        name,
        async request({ method, params }) {
          const body = { method, params };

          const { schedule } = createBatchScheduler({
            id: `${url}`,
            wait,
            shouldSplitBatch(requests) {
              return requests.length > batchSize;
            },
            fn: (body: RpcRequest[]) =>
              rpcHttp(url_, {
                body,
                fetchOptions,
                timeout,
              }),
          });

          const fn = async (body: RpcRequest) => (batch ? schedule(body) : [await rpcHttp(url_, { body, fetchOptions, timeout })]);

          const [{ error, result }] = await fn(body);
          if (error)
            throw new RpcRequestError({
              body,
              error,
              url: url_,
            });
          return result;
        },
        retryCount,
        retryDelay,
        timeout,
        type: "http",
      },
      {
        url,
      },
    );
  };
}
