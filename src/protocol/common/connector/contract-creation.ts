import axios from "axios";
import { isArray, isString } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { EXPLORER_URLS, MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS } from "../../../utils/config";
import { MultiChainEtherscanProvider } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";

const logger = rootLogger.child({ module: "connector-common", component: "contract-creation" });

interface ContractCallParams {
  contractAddress: string;
  chain: Chain;
}

export interface ContractCreationInfos {
  blockNumber: number;
  datetime: Date;
}

export function fetchContractCreationInfos$<TObj, TParams extends ContractCallParams, TRes>(options: {
  rpcConfig: RpcConfig;
  getCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: ContractCreationInfos | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // make sure we don't hit the rate limit of the explorers
    rateLimit$(MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS),

    Rx.mergeMap(async (obj: TObj) => {
      const param = options.getCallParams(obj);
      try {
        const result = await getContractCreationInfos(options.rpcConfig, param.contractAddress, param.chain);
        return options.formatOutput(obj, result);
      } catch (error) {
        logger.error({ msg: "Error while fetching contract creation block", data: { obj, error: error } });
        logger.error(error);
        return options.formatOutput(obj, null);
      }
    }, 1 /* concurrency */),
  );
}

async function getContractCreationInfos(rpcConfig: RpcConfig, contractAddress: string, chain: Chain): Promise<ContractCreationInfos> {
  const blockScoutChainsTimeout: Set<Chain> = new Set(["fuse", "metis", "celo", "emerald", "kava"]);
  const harmonyRpcChains: Set<Chain> = new Set(["harmony"]);

  if (blockScoutChainsTimeout.has(chain)) {
    logger.trace({
      msg: "BlockScout explorer detected for this chain, proceeding to scrape",
      data: { contractAddress, chain },
    });
    return await getBlockScoutScrapingContractCreationInfos(contractAddress, EXPLORER_URLS[chain], chain);
  } else if (harmonyRpcChains.has(chain)) {
    logger.trace({
      msg: "Using Harmony RPC method for this chain",
      data: { contractAddress, chain },
    });
    return await getHarmonyRpcCreationInfos(rpcConfig, contractAddress, chain);
  } else if (MultiChainEtherscanProvider.isChainSupported(chain)) {
    // we also use explorers for other things so we want to globally rate limit them
    const etherscanConfig = rpcConfig.etherscan;
    if (!etherscanConfig) {
      throw new ProgrammerError("Etherscan is not configured for this chain");
    }
    return callLockProtectedRpc(() => getFromExplorerCreationInfos(contractAddress, EXPLORER_URLS[chain]), {
      chain,
      logInfos: { msg: "Fetching contract creation block", data: { contractAddress, chain } },
      maxTotalRetryMs: 10_000,
      rpcLimitations: etherscanConfig.limitations,
      provider: etherscanConfig.provider,
      noLockIfNoLimit: true, // etherscan have a rate limit so this has no effect
    });
  } else {
    return await getFromExplorerCreationInfos(contractAddress, EXPLORER_URLS[chain]);
  }
}
async function getFromExplorerCreationInfos(contractAddress: string, explorerUrl: string) {
  const params = {
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
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, explorerUrl, params } });

  try {
    const resp = await axios.get(explorerUrl, { params });

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

    return { blockNumber, datetime };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, url, chain, error: error } });
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
