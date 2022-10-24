import { deepCopy } from "@ethersproject/properties";
import { fetchJson } from "@ethersproject/web";
import AsyncLock from "async-lock";
import * as ethers from "ethers";
import { backOff } from "exponential-backoff";
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
        // retryable errors are logged at a higher level
        logger.trace({ msg: "RPC error", data: { request: event.request, error: event.error, rpcUrl: safeToLogUrl } });
      }
    },
  );
}

// until this is fixed: https://github.com/ethers-io/ethers.js/issues/2749#issuecomment-1268638214
export function monkeyPatchEthersBatchProvider(provider: ethers.providers.JsonRpcBatchProvider) {
  logger.trace({ msg: "Patching ethers batch provider" });

  function fixedBatchSend(this: typeof provider, method: string, params: Array<any>): Promise<any> {
    const request = {
      method: method,
      params: params,
      id: this._nextId++,
      jsonrpc: "2.0",
    };

    if (this._pendingBatch == null) {
      this._pendingBatch = [];
    }

    const inflightRequest: any = { request, resolve: null, reject: null };

    const promise = new Promise((resolve, reject) => {
      inflightRequest.resolve = resolve;
      inflightRequest.reject = reject;
    });

    this._pendingBatch.push(inflightRequest);

    if (!this._pendingBatchAggregator) {
      // Schedule batch for next event loop + short duration
      this._pendingBatchAggregator = setTimeout(() => {
        // Get teh current batch and clear it, so new requests
        // go into the next batch
        const batch = this._pendingBatch;
        // @ts-ignore
        this._pendingBatch = null;
        // @ts-ignore
        this._pendingBatchAggregator = null;

        // Get the request as an array of requests
        const request = batch.map((inflight) => inflight.request);

        this.emit("debug", {
          action: "requestBatch",
          request: deepCopy(request),
          provider: this,
        });

        return fetchJson(this.connection, JSON.stringify(request))
          .then((result) => {
            this.emit("debug", {
              action: "response",
              request: request,
              response: result,
              provider: this,
            });

            if (!Array.isArray(result)) {
              if (result.error) {
                const error = new Error(result.error.message);
                (error as any).code = result.error.code;
                (error as any).data = result.error.data;
                throw error;
              } else {
                throw new Error("Batch result is not an array");
              }
            }

            // For each result, feed it to the correct Promise, depending
            // on whether it was a success or error
            batch.forEach((inflightRequest, index) => {
              const payload = result[index];
              if (payload.error) {
                const error = new Error(payload.error.message);
                (error as any).code = payload.error.code;
                (error as any).data = payload.error.data;
                inflightRequest.reject(error);
              } else {
                inflightRequest.resolve(payload.result);
              }
            });
          })
          .catch((error) => {
            this.emit("debug", {
              action: "response",
              error: error,
              request: request,
              provider: this,
            });

            batch.forEach((inflightRequest) => {
              inflightRequest.reject(error);
            });
          });
      }, 10);
    }

    return promise;
  }

  provider.send = fixedBatchSend.bind(provider);
}

/**
 * Harmony RPC returns nonsense when the call id is not 1.
 * Ex:
 *   {"jsonrpc":"2.0","method":"hmyv2_getTransactionsHistory","params":[{"address":"0x6ab6d61428fde76768d7b45d8bfeec19c6ef91a8","pageIndex":0,"pageSize":1,"fullTx":true,"txType":"ALL","order":"ASC"}],"id":1}
 *    -> OK
 *   {"jsonrpc":"2.0","method":"hmyv2_getTransactionsHistory","params":[{"address":"0x6ab6d61428fde76768d7b45d8bfeec19c6ef91a8","pageIndex":0,"pageSize":1,"fullTx":true,"txType":"ALL","order":"ASC"}],"id":42}
 *    -> empty list
 */
export function monkeyPatchHarmonyLinearProvider(provider: ethers.providers.JsonRpcProvider) {
  logger.trace({ msg: "Patching Harmony linear provider" });

  // override the id property so it's always 1
  Object.defineProperty(provider, "_nextId", {
    get: () => 1,
    set: () => {},
    enumerable: false,
    configurable: false,
  });
}

/**
 * Moonbeam RPC have caching issues
 * Ex:
 *   {"method":"eth_getBlockByNumber","params":["0x1c785f",false],"id":1,"jsonrpc":"2.0"}
 *    - when response header contains "cf-cache-status: DYNAMIC", the response is wrong
 *        -> {"jsonrpc":"2.0","result":null,"id":1}
 *    - when response header contains "cf-cache-status: HIT", the response is correct
 *        -> {"jsonrpc":"2.0","result":{"author":"0x19 ...
 */
export function monkeyPatchMoonbeamLinearProvider(provider: ethers.providers.JsonRpcProvider) {
  logger.trace({ msg: "Patching Moonbeam linear provider" });

  const chainLock = new AsyncLock({
    // max amount of time an item can remain in the queue before acquiring the lock
    timeout: 0, // never
    // we don't want a lock to be reentered
    domainReentrant: false,
    //max amount of time allowed between entering the queue and completing execution
    maxOccupationTime: 0, // never
    // max number of tasks allowed in the queue at a time
    maxPending: 100_000,
  });

  const originalSend = provider.send.bind(provider);

  class ShouldRetryException extends Error {}

  function getResult(payload: { error?: { code?: number; data?: any; message?: string }; result?: any }): any {
    if (payload.error) {
      // @TODO: not any
      const error: any = new Error(payload.error.message);
      error.code = payload.error.code;
      error.data = payload.error.data;
      throw error;
    }

    return payload.result;
  }

  async function sendButRetryOnBlockNumberCacheHit(this: ethers.providers.JsonRpcProvider, method: string, params: Array<any>): Promise<any> {
    if (method !== "eth_getBlockByNumber") {
      return originalSend(method, params);
    }

    const result = await chainLock.acquire("moonbeam", () =>
      backOff(
        async () => {
          const result = await originalSend(method, params);

          if (result === null) {
            logger.trace({ msg: "Got null result from eth_getBlockByNumber", data: { chain: "moonbeam", params } });
            throw new ShouldRetryException("Got null result from eth_getBlockByNumber");
          }
          return result;
        },
        {
          delayFirstAttempt: false,
          startingDelay: 500,
          timeMultiple: 5,
          maxDelay: 1_000,
          numOfAttempts: 10,
          jitter: "full",
          retry: (error) => {
            if (error instanceof ShouldRetryException) {
              return true;
            }
            return false;
          },
        },
      ),
    );

    return result;
  }

  // override the id property so it's always 1 to maximize cache hits
  /*Object.defineProperty(provider, "_nextId", {
    get: () => 1,
    set: () => {},
    enumerable: false,
    configurable: false,
  });*/

  provider.send = sendButRetryOnBlockNumberCacheHit.bind(provider);
}
