import { BlockTag, Filter, FilterByBlockHash, Log } from "@ethersproject/abstract-provider";
import { deepCopy, resolveProperties, shallowCopy } from "@ethersproject/properties";
import { Formatter } from "@ethersproject/providers";
import { fetchJson } from "@ethersproject/web";
import AsyncLock from "async-lock";
import * as ethers from "ethers";
import { Logger as EthersLogger } from "ethers/lib/utils";
import { backOff } from "exponential-backoff";
import { get, isString } from "lodash";
import { Chain } from "../types/chain";
import { sleep } from "./async";
import { rootLogger } from "./logger";
import { isArchiveNodeNeededError } from "./rpc/archive-node-needed";
import { removeSecretsFromRpcUrl } from "./rpc/remove-secrets-from-rpc-url";
import { MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS } from "./rpc/rpc-limitations";

const logger = rootLogger.child({ module: "utils", component: "ethers" });

export function normalizeAddressOrThrow(address: string) {
  // special case to avoid ethers.js throwing an error
  // Error: invalid address (argument="address", value=Uint8Array(0x0000000000000000000000000000000000000000), code=INVALID_ARGUMENT, version=address/5.6.1)
  if (address === "0x0000000000000000000000000000000000000000") {
    return address;
  }
  // this will throw on invalid addresses
  return ethers.utils.getAddress(address);
}

export type EthersProviderDebugEvent =
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
    }
  | {
      action: "custom";
      data: any;
    };
export function addDebugLogsToProvider(
  provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider | ethers.providers.EtherscanProvider,
) {
  const url = provider instanceof ethers.providers.EtherscanProvider ? provider.getBaseUrl() : provider.connection.url;
  const safeToLogUrl = removeSecretsFromRpcUrl(url);
  provider.on("debug", (event: EthersProviderDebugEvent) => {
    if (event.action === "request" || event.action === "requestBatch") {
      logger.trace({ msg: "RPC request", data: { request: event.request, rpcUrl: safeToLogUrl } });
    } else if (event.action === "response" && "error" in event) {
      // retryable errors are logged at a higher level
      logger.trace({ msg: "RPC error", data: { request: event.request, error: event.error, rpcUrl: safeToLogUrl } });
    } else if (event.action === "response" && "response" in event) {
      logger.trace({ msg: "RPC response", data: { request: event.request, response: event.response, rpcUrl: safeToLogUrl } });
    } else if (event.action === "custom") {
      logger.trace({ msg: "RPC custom", data: { data: event.data, rpcUrl: safeToLogUrl } });
    }
  });
}

/**
 * some RPC are in fact clusters of archive and non-archive nodes
 * sometimes we hit a non-archive node and it fails but we can retry to hope we hit an archive node
 */
export function monkeyPatchArchiveNodeRpcProvider(provider: ethers.providers.JsonRpcProvider, retryDelay: number) {
  logger.trace({ msg: "Monkey patching archive node RPC provider", data: { rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) } });
  const originalSend = provider.send.bind(provider);

  provider.send = async (method: string, params: any[]) => {
    if (get(provider, "__disableRetryArchiveNodeErrors", false)) {
      return originalSend(method, params);
    }

    let attemptsRemaining = MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS;
    let lastError: any;
    while (attemptsRemaining-- > 0) {
      try {
        const result = await originalSend(method, params);
        return result;
      } catch (e) {
        if (isArchiveNodeNeededError(e)) {
          lastError = e;
          logger.debug({
            msg: "RPC archive node error on an archive node, will retry",
            data: { error: e, attemptsRemaining, rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) },
          });
          logger.debug(e);
          await sleep(retryDelay);
        } else {
          throw e;
        }
      }
    }
    logger.error({
      msg: "RPC archive node error after all retries, consider setting this rpc as being a non-archive node",
      data: { error: lastError, attemptsRemaining, rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) },
    });
    logger.error(lastError);
    throw lastError;
  };
}

// until this is fixed: https://github.com/ethers-io/ethers.js/issues/2749#issuecomment-1268638214
// also fix unordered batch requests https://github.com/ethers-io/ethers.js/pull/2657
export function monkeyPatchEthersBatchProvider(provider: ethers.providers.JsonRpcBatchProvider) {
  logger.trace({ msg: "Patching ethers batch provider" });

  interface RpcResult {
    jsonrpc: "2.0";
    id: number;
    result?: string;
    error?: {
      code: number;
      message: string;
      data?: any;
    };
  }

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
          .then((result: RpcResult[] | RpcResult) => {
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

            const resultMap = result.reduce((resultMap, payload) => {
              resultMap[payload.id] = payload;
              return resultMap;
            }, {} as Record<number, RpcResult>);

            // For each result, feed it to the correct Promise, depending
            // on whether it was a success or error
            batch.forEach((inflightRequest, index) => {
              const payload = resultMap[inflightRequest.request.id];
              if (payload.error) {
                const error = new Error(payload.error.message);
                (error as any).code = payload.error.code;
                (error as any).data = payload.error.data;

                this.emit("debug", {
                  action: "response",
                  error: payload,
                  request: request,
                  provider: this,
                });

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
 * Harmony RPC returns empty values sometimes
 * At first we thought it was because of the jsonrpc id
 * But it seems to be a bug in the RPC itself where it returns empty values
 * Happens to both batch and non batch requests, so we patch both
 * Spotted on eth_getTransactionReceipt and hmyv2_getTransactionsHistory
 *
 * Ex:
 *   {"jsonrpc":"2.0","method":"hmyv2_getTransactionsHistory","params":[{"address":"0x6ab6d61428fde76768d7b45d8bfeec19c6ef91a8","pageIndex":0,"pageSize":1,"fullTx":true,"txType":"ALL","order":"ASC"}],"id":1}
 *    -> OK
 *   {"jsonrpc":"2.0","method":"hmyv2_getTransactionsHistory","params":[{"address":"0x6ab6d61428fde76768d7b45d8bfeec19c6ef91a8","pageIndex":0,"pageSize":1,"fullTx":true,"txType":"ALL","order":"ASC"}],"id":42}
 *    -> empty list
 */
export function monkeyPatchHarmonyProviderRetryNullResponses(provider: ethers.providers.JsonRpcProvider) {
  logger.trace({ msg: "Patching Harmony linear provider" });

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

  async function sendButRetryOnUnsatisfyingResponse(this: ethers.providers.JsonRpcProvider, method: string, params: Array<any>): Promise<any> {
    if (method !== "eth_getBlockByNumber" && method !== "eth_getTransactionReceipt" && method !== "hmyv2_getTransactionsHistory") {
      return originalSend(method, params);
    }

    const result = await chainLock.acquire("harmony", () =>
      backOff(
        async () => {
          const result = await originalSend(method, params);

          if (result === null) {
            logger.trace({ msg: "Got null result from method", data: { chain: "harmony", params, method } });
            throw new ShouldRetryException("Got null result from " + method);
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

  provider.send = sendButRetryOnUnsatisfyingResponse.bind(provider);
}

/**
 * Celo RPC doesn't provide some mandatory response fields
 * https://github.com/ethers-io/ethers.js/issues/1735#issuecomment-1016079512
 * This is less invasive than the celo-ethers-wrapper package that is not able to handle batch requests since it also overrides the send method
 */
export function monkeyPatchCeloProvider(provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider) {
  // Override certain block formatting properties that don't exist on Celo blocks
  // Reaches into https://github.com/ethers-io/ethers.js/blob/master/packages/providers/src.ts/formatter.ts
  const blockFormat = provider.formatter.formats.block;
  blockFormat.gasLimit = () => ethers.BigNumber.from(0);
  blockFormat.nonce = () => "";
  blockFormat.difficulty = () => 0;

  const blockWithTransactionsFormat = provider.formatter.formats.blockWithTransactions;
  blockWithTransactionsFormat.gasLimit = () => ethers.BigNumber.from(0);
  blockWithTransactionsFormat.nonce = () => "";
  blockWithTransactionsFormat.difficulty = () => 0;

  const transactionFormat = provider.formatter.formats.transaction;
  transactionFormat.gasLimit = () => ethers.BigNumber.from(0);
}

/**
 * Optimism gas structure is different from other chains
 */
export function monkeyPatchLayer2ReceiptFormat(provider: ethers.providers.JsonRpcProvider) {
  // Override certain receipt formatting properties that only exist on Optimism
  const bigNumberFormatter = ethers.providers.Formatter.allowNull((value: string) => {
    // this is for metis, where some values can be decimal point strings instead of hex
    // {"method":"eth_getTransactionReceipt","params":["0x4154d683c5d964963fca3f6d065b501c70bafbb06df009109186fbb6014fe310"],"id":120,"jsonrpc":"2.0"}
    //   -> "l1FeeScalar": "17.5",
    // we just truncate the decimal point
    if (isString(value) && !value.startsWith("0x") && value.includes(".")) {
      value = value.split(".")[0];
    }
    return provider.formatter.bigNumber(value);
  }, null);

  const receiptFormat = provider.formatter.formats.receipt;
  receiptFormat.effectiveGasPrice = () => ethers.BigNumber.from(0); // effective gas price is not provided by Optimism rpc
  receiptFormat.l1Fee = bigNumberFormatter;
  receiptFormat.l1FeeScalar = bigNumberFormatter;
  receiptFormat.l1GasPrice = bigNumberFormatter;
  receiptFormat.l1GasUsed = bigNumberFormatter;
}

/**
 * Harmony gas structure is different from other chains
 */
export function monkeyPatchMissingEffectiveGasPriceReceiptFormat(provider: ethers.providers.JsonRpcProvider) {
  const receiptFormat = provider.formatter.formats.receipt;
  receiptFormat.effectiveGasPrice = () => ethers.BigNumber.from(0);
}

/**
 * Sometimes we are getting an "underlying network changed" error
 * most likely some disconnect on the RPC side
 * we just retry the request
 */
export function monkeyPatchProviderToRetryUnderlyingNetworkChangedError(provider: ethers.providers.JsonRpcProvider, retryDelay: number) {
  logger.trace({ msg: "Patching provider to retry underlying network changed error" });
  const originalSend = provider.send.bind(provider);
  let attemptsRemaining = 10;
  let lastError: Error | undefined = undefined;
  provider.send = async function send(method: string, params: Array<any>): Promise<any> {
    try {
      const result = await originalSend(method, params);
      return result;
    } catch (error: any) {
      if (attemptsRemaining-- > 0 && get(error, "message", "").includes("underlying network changed")) {
        lastError = error;
        logger.warn({
          msg: "Got underlying network changed error, retrying",
          data: { attemptsRemaining, error, rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) },
        });
        await sleep(retryDelay);
      } else {
        throw error;
      }
    }
    logger.error({
      msg: "Got underlying network changed error, but no more attempts remaining",
      data: { attemptsRemaining, error: lastError, rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) },
    });
    throw lastError;
  };
}

/**
 * Idk why but ethers has a hardcoded url for etherscan for eth mainnet only
 * This class adds support for other networks
 */
export class MultiChainEtherscanProvider extends ethers.providers.EtherscanProvider {
  static isChainSupported(chain: Chain) {
    return ["bsc", "avax", "fantom", "ethereum", "polygon", "arbitrum", "optimism"].includes(chain);
  }

  override getBaseUrl(): string {
    switch (this.network ? this.network.name : "invalid") {
      case "bsc":
        return "https://api.bscscan.com";
      case "fantom":
        return "https://api.ftmscan.com";
      case "avax":
        return "https://api.snowtrace.io";
      case "homestead":
      case "ethereum":
        return "https://api.etherscan.io";
      case "polygon":
      case "matic":
        return "https://api.polygonscan.com";
      case "arbitrum":
        return "https://api.arbiscan.io";
      case "optimism":
        return "https://api-optimistic.etherscan.io";
      default:
    }

    return ethers.logger.throwArgumentError("unsupported network", "network", this.network.name);
  }
}

/**
 * Ankr has issues with their RPC, sometimes it returns an error like "we can't execute this request"
 * It turns out it's just a timeout, so we just retry the request
 */
export function monkeyPatchAnkrBscLinearProvider(provider: ethers.providers.JsonRpcProvider, retryDelay: number) {
  logger.trace({ msg: "Patching Ankr BSC linear provider" });

  const originalSend = provider.send.bind(provider);
  let attemptsRemaining = 50;
  let lastError: Error | undefined = undefined;
  provider.send = async function send(method: string, params: Array<any>): Promise<any> {
    try {
      const result = await originalSend(method, params);
      return result;
    } catch (error: any) {
      if (attemptsRemaining-- > 0 && get(error, "message", "").includes("we can't execute this request")) {
        lastError = error;
        logger.warn({
          msg: "Got we can't execute this request error, retrying",
          data: { attemptsRemaining, error, rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) },
        });
        await sleep(retryDelay + 100);
      } else {
        throw error;
      }
    }
    logger.error({
      msg: "Got we can't execute this request, but no more attempts remaining",
      data: { attemptsRemaining, error: lastError, rpcUrl: removeSecretsFromRpcUrl(provider.connection.url) },
    });
    throw lastError;
  };
}

/**
 * This is a hack to get acces to private methods of ethers.providers.JsonRpcProvider
 * so we can transform the Log objects we get from manual RPC calls
 * to ethers.Event objects. This is needed to be able to use the
 * address batching feature of some RPCs while we wait for ethers v6
 */

export interface MultiAddressEventFilter {
  address: string[];
  topics?: Array<string | Array<string>>;
  fromBlock?: BlockTag;
  toBlock?: BlockTag;
  blockHash?: string;
}

const ethersLogger = new EthersLogger("JsonRpcProviderWithMultiAddressGetLogs");
export class JsonRpcProviderWithMultiAddressGetLogs extends ethers.providers.JsonRpcProvider {
  async getLogsMultiAddress(filter: MultiAddressEventFilter): Promise<Array<Log>> {
    await this.getNetwork();
    const params = await resolveProperties({ filter: this.__getFilter(filter) });

    const logs: Log[] = await this.send("eth_getLogs", [params.filter]);

    logs.forEach((log) => {
      if (log.removed == null) {
        log.removed = false;
      }
    });

    return Formatter.arrayOf(this.formatter.filterLog.bind(this.formatter))(logs);
  }

  async __getFilter(filter: MultiAddressEventFilter): Promise<Filter | FilterByBlockHash> {
    filter = await filter;

    const result: any = {};

    if (filter.address != null) {
      result.address = Promise.all(filter.address.map(this.__getAddress));
    }

    ["blockHash", "topics"].forEach((key) => {
      if ((<any>filter)[key] == null) {
        return;
      }
      result[key] = (<any>filter)[key];
    });

    ["fromBlock", "toBlock"].forEach((key) => {
      if ((<any>filter)[key] == null) {
        return;
      }
      result[key] = this._getBlockTag((<any>filter)[key]);
    });

    const filterFormat = { ...this.formatter.formats.filter, address: Formatter.arrayOf(this.formatter.address.bind(this.formatter)) };
    const filterFormatter = (value: any): any => Formatter.check(filterFormat, value);

    return filterFormatter(await resolveProperties(result));
  }

  async __getAddress(addressOrName: string | Promise<string>): Promise<string> {
    addressOrName = await addressOrName;
    if (typeof addressOrName !== "string") {
      ethersLogger.throwArgumentError("invalid address or ENS name", "name", addressOrName);
    }

    const address = addressOrName;
    return address;
  }
}
export class ContractWithMultiAddressGetLogs extends ethers.Contract {
  declare readonly provider: JsonRpcProviderWithMultiAddressGetLogs;

  constructor(addressOrName: string, contractInterface: ethers.ContractInterface, signerOrProvider?: JsonRpcProviderWithMultiAddressGetLogs) {
    super(addressOrName, contractInterface, signerOrProvider);
  }

  public queryFilterMultiAddress(
    event: MultiAddressEventFilter,
    fromBlockOrBlockhash?: BlockTag | string,
    toBlock?: BlockTag,
  ): ReturnType<ethers.Contract["queryFilter"]> {
    const runningEvent = this.__getRunningEvent(event);
    const filter = shallowCopy<MultiAddressEventFilter>(runningEvent.filter);
    filter.address = event.address;

    if (typeof fromBlockOrBlockhash === "string" && ethers.utils.isHexString(fromBlockOrBlockhash, 32)) {
      if (toBlock != null) {
        ethersLogger.throwArgumentError("cannot specify toBlock with blockhash", "toBlock", toBlock);
      }
      filter.blockHash = fromBlockOrBlockhash;
    } else {
      filter.fromBlock = fromBlockOrBlockhash != null ? fromBlockOrBlockhash : 0;
      filter.toBlock = toBlock != null ? toBlock : "latest";
    }

    return this.provider.getLogsMultiAddress(filter).then((logs) => {
      return logs.map((log) => this.__wrapEvent(runningEvent, log, null as any));
    });
  }

  /**
   * make some private method callable from our class
   * https://stackoverflow.com/a/48908067/2523414
   *
   * Technically, in current versions of TypeScript private methods are only compile-time checked to be private - so you can call them.
   */
  protected __wrapEvent(runningEvent: any, log: Log, listener: any) {
    // @ts-ignore
    return super._wrapEvent(runningEvent, log, listener);
  }
  private __getRunningEvent(eventName: MultiAddressEventFilter | string): any {
    // @ts-ignore
    return super._getRunningEvent(eventName);
  }
}
