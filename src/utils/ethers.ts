import * as ethers from "ethers";
import { rootLogger } from "./logger";
import { removeSecretsFromRpcUrl } from "./rpc/remove-secrets-from-rpc-url";

const logger = rootLogger.child({ module: "utils", component: "ethers" });

export function normalizeAddress(address: string) {
  // special case to avoid ethers.js throwing an error
  // Error: invalid address (argument="address", value=Uint8Array(0x0000000000000000000000000000000000000000), code=INVALID_ARGUMENT, version=address/5.6.1)
  if (address === "0x0000000000000000000000000000000000000000") {
    return address;
  }
  return ethers.utils.getAddress(address);
}

export function addDebugLogsToProvider(provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider) {
  const safeToLogUrl = removeSecretsFromRpcUrl(provider.connection.url);
  provider.on(
    "debug",
    (
      event:
        | { action: "request"; request: any }
        | {
            action: "requestBatch";
            request: any;
          }
        | {
            action: "response";
            request: any;
            response: any;
          }
        | {
            action: "response";
            error: any;
            request: any;
          },
    ) => {
      if (event.action === "request" || event.action === "requestBatch") {
        logger.trace({ msg: "RPC request", data: { request: event.request, rpcUrl: safeToLogUrl } });
      } else if (event.action === "response" && "response" in event) {
        logger.trace({ msg: "RPC response", data: { request: event.request, response: event.response, rpcUrl: safeToLogUrl } });
      } else if (event.action === "response" && "error" in event) {
        logger.error({ msg: "RPC error", data: { request: event.request, error: event.error, rpcUrl: safeToLogUrl } });
      }
    },
  );
}
