import axios from "axios";
import { isArray, isString } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { samplingPeriodMs } from "../../../types/sampling";
import { sleep } from "../../../utils/async";
import { ETHERSCAN_API_KEY, EXPLORER_URLS } from "../../../utils/config";
import { MultiChainEtherscanProvider } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "connector-common", component: "contract-creation" });

interface ContractCallParams {
  contractAddress: string;
  chain: Chain;
}

interface ContractCreationInfos {
  blockNumber: number;
  datetime: Date;
}

export function fetchContractCreationInfos$<TObj, TParams extends ContractCallParams, TRes>(options: {
  ctx: ImportCtx;
  getCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: ContractCreationInfos | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // make sure we don't hit the rate limit of the explorers
    rateLimit$(samplingPeriodMs[options.ctx.behaviour.minDelayBetweenExplorerCalls]),

    Rx.mergeMap(async (obj: TObj) => {
      const param = options.getCallParams(obj);
      try {
        const result = await getContractCreationInfos(options.ctx, param.contractAddress, param.chain);
        return options.formatOutput(obj, result);
      } catch (error) {
        logger.error({ msg: "Error while fetching contract creation block", data: { obj, error: error } });
        logger.error(error);
        return options.formatOutput(obj, null);
      }
    }, 1 /* concurrency */),
  );
}

async function getContractCreationInfos(ctx: ImportCtx, contractAddress: string, chain: Chain): Promise<ContractCreationInfos> {
  const explorerType = EXPLORER_URLS[chain].type;
  if (explorerType === "blockscout") {
    logger.trace({
      msg: "BlockScout explorer detected for this chain, proceeding to scrape",
      data: { contractAddress, chain },
    });
    return await getBlockScoutScrapingContractCreationInfos(contractAddress, EXPLORER_URLS[chain].url, chain);
  } else if (explorerType === "blockscout-json") {
    logger.trace({
      msg: "BlockScout explorer detected for this chain, proceeding to scrape",
      data: { contractAddress, chain },
    });
    return await getBlockScoutJSONAPICreationInfo(ctx, contractAddress, EXPLORER_URLS[chain].url, chain);
  } else if (explorerType === "blockscout-api-v2") {
    logger.trace({
      msg: "BlockScout explorer detected for this chain, proceeding to scrape",
      data: { contractAddress, chain },
    });
    return await blockscoutApiV2(contractAddress, EXPLORER_URLS[chain].url, chain);
  } else if (explorerType === "routescan") {
    return await getRouteScanAPICreationInfo(contractAddress, EXPLORER_URLS[chain].url, chain);
  } else if (explorerType === "harmony") {
    logger.trace({
      msg: "Using Harmony RPC method for this chain",
      data: { contractAddress, chain },
    });
    return await getHarmonyRpcCreationInfos(ctx.rpcConfig, contractAddress, chain);
  } else if (MultiChainEtherscanProvider.isChainSupported(chain)) {
    // we also use explorers for other things so we want to globally rate limit them
    const etherscanConfig = ctx.rpcConfig.etherscan;
    if (!etherscanConfig) {
      throw new ProgrammerError("Etherscan is not configured for this chain");
    }
    return callLockProtectedRpc(() => getFromExplorerCreationInfos(contractAddress, EXPLORER_URLS[chain].url, ETHERSCAN_API_KEY[chain]), {
      chain,
      logInfos: { msg: "Fetching contract creation block", data: { contractAddress, chain } },
      maxTotalRetryMs: 10_000,
      rpcLimitations: etherscanConfig.limitations,
      provider: etherscanConfig.provider,
      noLockIfNoLimit: true, // etherscan have a rate limit so this has no effect
    });
  } else if (explorerType === "etherscan") {
    return await getFromExplorerCreationInfos(contractAddress, EXPLORER_URLS[chain].url, ETHERSCAN_API_KEY[chain]);
  } else if (explorerType === "zksync") {
    return await getFromZksyncExplorerApi(contractAddress, EXPLORER_URLS[chain].url);
  } else if (explorerType === "seitrace") {
    return await getFromSeitraceExplorer(contractAddress, EXPLORER_URLS[chain].url);
  } else {
    throw new Error("Unsupported explorer type: " + explorerType);
  }
}

async function getRouteScanAPICreationInfo(contractAddress: string, explorerUrl: string, chain: Chain) {
  if (chain !== "avax") {
    throw new ProgrammerError("RouteScan is only supported for Avalanche");
  }

  //https://api.routescan.io/v2/network/mainnet/evm/43114/address/0x595786A3848B1de66C6056C87BA91977935fBC46/transactions?ecosystem=avalanche&includedChainIds=43114&categories=evm_tx&sort=asc&limit=1
  const params = {
    ecosystem: "avalanche",
    includedChainIds: 43114,
    categories: "evm_tx",
    sort: "asc",
    limit: 1,
  };
  const apiPath = `${explorerUrl}/v2/network/mainnet/evm/43114/address/${encodeURIComponent(contractAddress)}/transactions`;
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, apiPath, params } });
  try {
    const resp = await axios.get(apiPath, { params });

    if (!isArray(resp.data.items) || resp.data.items.length === 0) {
      logger.error({ msg: "No contract creation transaction found", data: { contractAddress, apiPath, params, data: resp.data } });
      throw new Error("No contract creation transaction found");
    }
    let blockNumber: number = resp.data.items[0].blockNumber;
    let timeStamp: string = resp.data.items[0].timestamp;
    const datetime = new Date(timeStamp);

    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, apiPath, params, error: error } });
    logger.error(error);
    throw error;
  }
}

async function getFromExplorerCreationInfos(contractAddress: string, explorerUrl: string, apiKey: string | null = null) {
  const rawParams = {
    module: "account",
    action: "txlist",
    address: contractAddress,
    startblock: 1,
    endblock: 999999999,
    page: 1,
    offset: 1,
    sort: "asc",
    limit: 1,
  };
  const params = apiKey ? { ...rawParams, apiKey } : rawParams;
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, explorerUrl, params } });

  try {
    const resp = await axios.get(explorerUrl, {
      params,
      headers: {
        "User-Agent": "Mozilla/5.0", // basescan only works when the user agent is set
      },
    });

    if (!isArray(resp.data.result) || resp.data.result.length === 0) {
      logger.error({ msg: "No contract creation transaction found", data: { contractAddress, explorerUrl, params, data: resp.data } });
      throw new Error("No contract creation transaction found");
    }
    let blockNumber: number | string = resp.data.result[0].blockNumber;
    if (isString(blockNumber)) {
      blockNumber = parseInt(blockNumber);
    }

    let timeStamp: number | string = resp.data.result[0].timeStamp;
    if (isString(timeStamp)) {
      timeStamp = parseInt(timeStamp);
    }
    const datetime = new Date(timeStamp * 1000);

    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, explorerUrl, params, error: error } });
    logger.error(error);
    throw error;
  }
}

async function getBlockScoutScrapingContractCreationInfos(contractAddress: string, explorerUrl: string, chain: Chain) {
  var url = explorerUrl + `/address/${contractAddress}`;

  try {
    logger.trace({ msg: "Fetching blockscout transaction link", data: { contractAddress, url } });
    const resp = await axios.get(url);

    console.log(resp.data);
    const tx = resp.data.split(`<a data-test="transaction_hash_link" href="/`)[1].split(`"`)[0];
    const trxUrl = `${explorerUrl}/${tx}/internal-transactions`;
    logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, trxUrl } });
    const txResp = await axios.get(trxUrl);

    // for some reason, celo has a different block url
    const blockSelector = `<a class="transaction__link" href="${chain === "celo" ? "/mainnet" : ""}/block/`;
    const blockNumberStr: string = txResp.data.split(blockSelector)[1].split(`"`)[0];
    const blockNumber = parseInt(blockNumberStr);

    const rawDateStr: string = txResp.data.split(`data-from-now="`)[1].split(`"`)[0];
    const datetime = new Date(Date.parse(rawDateStr));

    logger.trace({ msg: "Fetched contract creation block", data: { chain, contractAddress, blockNumber, datetime } });
    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, url, chain, error: error } });
    logger.error(error);
    throw error;
  }
}

async function getBlockScoutJSONAPICreationInfo(ctx: ImportCtx, contractAddress: string, explorerUrl: string, chain: Chain) {
  try {
    let data: { items: string[]; next_page_path: string | null } = {
      items: [],
      next_page_path: `/address/${contractAddress}/internal-transactions?type=JSON`,
    };
    while (data.next_page_path) {
      let url = explorerUrl + data.next_page_path;
      if (!url.includes("type=JSON")) {
        url = url + "&type=JSON";
      }

      logger.trace({ msg: "Fetching blockscout internal transactions", data: { contractAddress, url } });
      const resp = await axios.get(url);
      data = resp.data;
      await sleep(samplingPeriodMs[ctx.behaviour.minDelayBetweenExplorerCalls]);
    }
    // sometimes, the internal transaction log is empty
    if (data.items.length === 0) {
      logger.info({ msg: "No internal transactions found, fetching from trx log", data: { contractAddress, explorerUrl } });

      data = {
        items: [],
        next_page_path: `/address/${contractAddress}/transactions?type=JSON`,
      };
      while (data.next_page_path) {
        let url = explorerUrl + data.next_page_path;
        if (!url.includes("type=JSON")) {
          url = url + "&type=JSON";
        }
        logger.trace({ msg: "Fetching blockscout transactions", data: { contractAddress, url } });
        const resp = await axios.get(url);
        data = resp.data;
        await sleep(samplingPeriodMs[ctx.behaviour.minDelayBetweenExplorerCalls]);
      }
    }

    logger.trace({ msg: "Found the first blockscout transaction", data: { contractAddress } });
    const tx = data.items[data.items.length - 1];
    const blockNumberStr = tx.split(`href="/block/`)[1].split(`"`)[0];
    const blockNumber = parseInt(blockNumberStr);

    const rawDateStr: string = tx.split(`data-from-now="`)[1].split(`"`)[0];
    const datetime = new Date(Date.parse(rawDateStr));

    logger.trace({ msg: "Fetched contract creation block", data: { chain, contractAddress, blockNumber, datetime } });
    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, chain, error: error } });
    logger.error(error);
    throw error;
  }
}

async function blockscoutApiV2(contractAddress: string, explorerUrl: string, chain: Chain) {
  try {
    // https://explorer.mantle.xyz/api/v2/addresses/0x784A2843984EDcC4740648dC91E5C7444254a397
    // { ..., "creation_tx_hash": "0xa4b6d76d1d04f63e7f43c29b0885c9361af4ca3bda07f155e3f0ef8e78afa19f" }
    // then
    // https://explorer.mantle.xyz/api/v2/transactions/0xa4b6d76d1d04f63e7f43c29b0885c9361af4ca3bda07f155e3f0ef8e78afa19f
    // { ..., "timestamp": "2024-04-09T13:54:50.000000Z", "block": 62270289 }

    const addressUrl = new URL(explorerUrl);
    addressUrl.pathname = `/api/v2/addresses/${contractAddress}`;
    logger.trace({ msg: "Fetching contract details", data: { contractAddress, explorerUrl: addressUrl.href } });
    const addressResp = await axios.get<{ creation_tx_hash: string }>(addressUrl.href);
    const creationTxHash = addressResp.data?.creation_tx_hash;
    if (!creationTxHash) {
      logger.error({
        msg: "Could not find a valid transaction hash",
        data: { contractAddress, explorerUrl: addressUrl.href, data: addressResp.data },
      });
      throw new Error("Could not find a valid transaction hash");
    }

    // sleep a bit to avoid rate limiting
    await sleep(3000);

    const txUrl = new URL(explorerUrl);
    txUrl.pathname = `/api/v2/transactions/${creationTxHash}`;
    logger.trace({ msg: "Fetching transaction details", data: { contractAddress, explorerUrl: txUrl.href } });
    const txResp = await axios.get<{ timestamp: string; block: number }>(txUrl.href);
    const blockNumber = txResp.data?.block;
    const timeStamp = txResp.data?.timestamp;
    if (!blockNumber || !timeStamp) {
      logger.error({
        msg: "Could not find a valid block number or timestamp",
        data: { contractAddress, explorerUrl: txUrl.href, data: txResp.data },
      });
      throw new Error("Could not find a valid block number or timestamp");
    }

    const datetime = new Date(timeStamp);
    if (isNaN(datetime.getTime())) {
      logger.error({ msg: "Could not parse timestamp", data: { contractAddress, explorerUrl: txUrl.href, data: txResp.data } });
      throw new Error("Could not parse timestamp");
    }

    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, chain, error: error } });
    logger.error(error);
    throw error;
  }
}

async function getHarmonyRpcCreationInfos(rpcConfig: RpcConfig, contractAddress: string, chain: Chain) {
  const params = [
    {
      address: contractAddress,
      pageIndex: 0,
      pageSize: 1,
      fullTx: true,
      txType: "ALL",
      order: "ASC",
    },
  ];
  type TResp = { transactions: { blockNumber: number; timestamp: number }[] };

  try {
    const resp: TResp = await callLockProtectedRpc(() => rpcConfig.linearProvider.send("hmyv2_getTransactionsHistory", params), {
      chain: chain,
      provider: rpcConfig.linearProvider,
      rpcLimitations: rpcConfig.rpcLimitations,
      maxTotalRetryMs: 60 * 1000,
      logInfos: { msg: "getHarmonyRpcCreationInfos", data: { contractAddress, chain, params } },
      noLockIfNoLimit: true, // we don't use the batching mechanism for this call so yeah, skip locks if possible
    });

    // remove nulls
    const trxs = resp.transactions.filter((t) => t);
    if (!isArray(trxs)) {
      logger.error({ msg: "transaction response not an array", data: { contractAddress, data: resp } });
      throw new Error("transaction response not an array");
    }
    if (trxs.length <= 0) {
      logger.error({ msg: "Empty transaction array", data: { contractAddress, data: resp } });
      throw new Error("Empty transaction array");
    }

    const tx0 = trxs[0];
    return { blockNumber: tx0.blockNumber, datetime: new Date(tx0.timestamp * 1000) };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, params, chain, error: error } });
    logger.error(error);
    throw error;
  }
}

async function getFromZksyncExplorerApi(contractAddress: string, explorerUrl: string) {
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, explorerUrl } });

  const callUrl = new URL(explorerUrl);
  callUrl.pathname = `/address/${contractAddress}`;

  try {
    const contractResp = await axios.get<{ createdInBlockNumber: number }>(callUrl.href);
    const blockNumber = contractResp.data?.createdInBlockNumber;
    logger.trace({ msg: "Fetched contract creation block", data: { contractAddress, blockNumber } });
    if (!blockNumber) {
      logger.error({
        msg: "Could not find a valid block number",
        data: { contractAddress, explorerUrl: callUrl.href, data: contractResp.data },
      });
      throw new Error("Could not find a valid block number");
    }

    callUrl.pathname = `/blocks/${blockNumber}`;
    const blockResp = await axios.get<{ timestamp: string }>(callUrl.href);
    const timeStamp = blockResp.data?.timestamp;
    if (!timeStamp) {
      logger.error({ msg: "Could not find a valid timestamp", data: { contractAddress, explorerUrl: callUrl.href, data: blockResp.data } });
      throw new Error("Could not find a valid block number");
    }
    logger.trace({ msg: "Fetched contract creation block timestamp", data: { contractAddress, blockNumber, timeStamp } });
    const datetime = new Date(timeStamp);
    if (isNaN(datetime.getTime())) {
      logger.error({ msg: "Could not parse timestamp", data: { contractAddress, explorerUrl: callUrl.href, data: blockResp.data } });
      throw new Error("Could not parse timestamp");
    }

    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, explorerUrl, error: error } });
    logger.error(error);
    throw error;
  }
}

async function getFromSeitraceExplorer(contractAddress: string, explorerUrl: string) {
  var url = explorerUrl + `/pacific-1/gateway/api/v1/addresses/${contractAddress}`;

  try {
    logger.trace({ msg: "Fetching blockscout transaction link", data: { contractAddress, url } });
    const resp = await axios.get(url);

    const tx = resp.data.creation_tx_hash;
    const trxUrl = `${explorerUrl}/pacific-1/gateway/api/v1/transactions/${tx}`;

    const txResp = await axios.get(trxUrl);
    const blockNumber = txResp.data.block;
    const rawDateStr: string = txResp.data.timestamp;
    const datetime = new Date(Date.parse(rawDateStr));

    logger.trace({ msg: "Fetched contract creation block", data: { contractAddress, blockNumber, datetime } });
    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, url, error: error } });
    logger.error(error);
    throw error;
  }
}
