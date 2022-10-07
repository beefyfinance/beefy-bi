import axios from "axios";
import { isArray, isString, sample } from "lodash";
import { Chain } from "../../../types/chain";
import { EXPLORER_URLS, MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS, RPC_URLS } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import * as Rx from "rxjs";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { RpcConfig } from "../../../types/rpc-config";

const logger = rootLogger.child({ module: "connector-common", component: "contract-creation" });

interface ContractCallParams {
  contractAddress: string;
  chain: Chain;
}

export interface ContractCreationInfos {
  blockNumber: number;
}

export function fetchContractCreationBlock$<TObj, TParams extends ContractCallParams, TRes>(options: {
  rpcConfig: RpcConfig;
  getCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: ContractCreationInfos | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const getCreationBlock$ = Rx.mergeMap(async (obj: TObj) => {
    const param = options.getCallParams(obj);
    try {
      const result = await getContractCreationBlock(param.contractAddress, param.chain);
      return options.formatOutput(obj, result);
    } catch (error) {
      logger.error({ msg: "Error while fetching contract creation block", data: { obj, error: error } });
      logger.error(error);
      return options.formatOutput(obj, null);
    }
  }, 1 /* concurrency */);

  return Rx.pipe(
    Rx.groupBy((obj) => options.getCallParams(obj).chain),
    Rx.map((chainObjs$) =>
      chainObjs$.pipe(
        // make sure we don't hit the rate limit of the exploreres
        rateLimit$(MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS),

        getCreationBlock$,
      ),
    ),
    Rx.mergeAll(),
  );
}

async function getContractCreationBlock(contractAddress: string, chain: Chain): Promise<ContractCreationInfos> {
  if (blockScoutChainsTimeout.has(chain)) {
    logger.trace({
      msg: "BlockScout explorer detected for this chain, proceeding to scrape",
      data: { contractAddress, chain },
    });
    return await getBlockScoutScrapingContractCreationBlock(contractAddress, EXPLORER_URLS[chain], chain);
  } else if (harmonyRpcChains.has(chain)) {
    logger.trace({
      msg: "Using Harmony RPC method for this chain",
      data: { contractAddress, chain },
    });
    return await getHarmonyRpcCreationBlock(contractAddress, chain);
  } else {
    return await getFromExplorerCreationBlock(contractAddress, EXPLORER_URLS[chain]);
  }
}

const blockScoutChainsTimeout: Set<Chain> = new Set(["fuse", "metis", "celo", "emerald"]);
const harmonyRpcChains: Set<Chain> = new Set(["harmony"]);

async function getFromExplorerCreationBlock(contractAddress: string, explorerUrl: string) {
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

  const resp = await axios.get(explorerUrl, { params });

  if (!isArray(resp.data.result) || resp.data.result.length === 0) {
    logger.error({ msg: "No contract creation transaction found", data: { contractAddress, explorerUrl, params, data: resp.data } });
    throw new Error("No contract creation transaction found");
  }
  let blockNumber: number | string = resp.data.result[0].blockNumber;

  if (isString(blockNumber)) {
    blockNumber = parseInt(blockNumber);
  }

  return { blockNumber };
}

async function getBlockScoutScrapingContractCreationBlock(contractAddress: string, explorerUrl: string, chain: Chain) {
  var url = explorerUrl + `/address/${contractAddress}`;

  try {
    logger.trace({ msg: "Fetching blockscout transaction link", data: { contractAddress, url } });
    const resp = await axios.get(url);

    const tx = resp.data.split(`<a data-test="transaction_hash_link" href="/`)[1].split(`"`)[0];
    const trxUrl = `${explorerUrl}/${tx}/internal-transactions`;
    logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, trxUrl } });
    const txResp = await axios.get(trxUrl);

    const blockNumberStr: string = txResp.data.split(`<a class="transaction__link" href="/block/`)[1].split(`"`)[0];
    const blockNumber = parseInt(blockNumberStr);

    return { blockNumber };
  } catch (error) {
    logger.error({ msg: "Error while fetching contract creation block", data: { contractAddress, url, error: error } });
    throw error;
  }
}

async function getHarmonyRpcCreationBlock(contractAddress: string, chain: Chain) {
  const rpcUrl = sample(RPC_URLS[chain]) as string;
  const rpcPayload = {
    jsonrpc: "2.0",
    method: "hmyv2_getTransactionsHistory",
    params: [
      {
        address: contractAddress,
        pageIndex: 0,
        pageSize: 1,
        fullTx: true,
        txType: "ALL",
        order: "ASC",
      },
    ],
    id: 1,
  };
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, rpcUrl, rpcPayload } });
  const resp = await axios.post(rpcUrl, rpcPayload);

  if (!resp.data || resp.data.id !== 1 || !resp.data.result || !resp.data.result.transactions || resp.data.result.transactions.length !== 1) {
    logger.error({
      msg: "Error while fetching contract creation block: Malformed response",
      data: { contractAddress, chain },
    });
    logger.error(resp.data);
    throw new Error("Malformed response");
  }

  if (!isArray(resp.data.result.transactions)) {
    logger.error({ msg: "Empty transaction array", data: { contractAddress, rpcUrl, data: resp.data } });
    throw new Error("Empty transaction array");
  }
  const transactions = resp.data.result.transactions.filter((tx: any) => tx); // remove nulls
  if (transactions.length === 0) {
    logger.error({ msg: "Empty transaction array", data: { contractAddress, rpcUrl, data: resp.data } });
    throw new Error("Empty transaction array");
  }

  const tx0 = transactions[0];
  let blockNumber: string | number = tx0.blockNumber;
  if (isString(blockNumber)) {
    blockNumber = parseInt(blockNumber);
  }

  return { blockNumber };
}
