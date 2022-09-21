import axios from "axios";
import { ethers } from "ethers";
import { isArray, sample } from "lodash";
import { Chain } from "../../../types/chain";
import { RPC_URLS } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import * as Rx from "rxjs";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";

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
  provider: ethers.providers.JsonRpcProvider;
  getCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: { blockNumber: number; datetime: Date } | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.groupBy((obj) => options.getCallParams(obj).chain),
    Rx.map((chainObjs$) =>
      chainObjs$.pipe(
        // make sure we don't hit the rate limit of the exploreres
        rateLimit$(10000),

        Rx.mergeMap(async (obj) => {
          const param = options.getCallParams(obj);
          try {
            const result = await getContractCreationInfos(param.contractAddress, chainObjs$.key);
            return options.formatOutput(obj, result);
          } catch (error) {
            logger.error({ msg: "Error while fetching contract creation block", data: { obj, error: error } });
            logger.error(error);
            return options.formatOutput(obj, null);
          }
        }),
      ),
    ),
    Rx.mergeAll(),
  );
}

async function getContractCreationInfos(contractAddress: string, chain: Chain): Promise<ContractCreationInfos> {
  if (blockScoutChainsTimeout.has(chain)) {
    logger.trace({
      msg: "BlockScout explorer detected for this chain, proceeding to scrape",
      data: { contractAddress, chain },
    });
    return await getCreationTimestampBlockScoutScraping(contractAddress, explorerApiUrls[chain], chain);
  } else if (harmonyRpcChains.has(chain)) {
    logger.trace({
      msg: "Using Harmony RPC method for this chain",
      data: { contractAddress, chain },
    });
    return await getCreationTimestampHarmonyRpc(contractAddress, chain);
  } else {
    return await getCreationBlockFromExplorer(contractAddress, explorerApiUrls[chain]);
  }
}

const explorerApiUrls: { [chain in Chain]: string } = {
  cronos: "https://api.cronoscan.com/api",
  bsc: "https://api.bscscan.com/api",
  polygon: "https://api.polygonscan.com/api",
  fantom: "https://api.ftmscan.com/api",
  heco: "https://api.hecoinfo.com/api",
  avax: "https://api.snowtrace.io//api",
  moonbeam: "https://api-moonbeam.moonscan.io/api",
  celo: "https://explorer.celo.org/",
  moonriver: "https://api-moonriver.moonscan.io/api",
  arbitrum: "https://api.arbiscan.io/api",
  aurora: "https://api.aurorascan.dev/api",
  metis: "https://andromeda-explorer.metis.io/",
  harmony: "https://explorer.harmony.one/",
  fuse: "https://explorer.fuse.io/",
  emerald: "https://explorer.emerald.oasis.dev/",
  optimism: "https://api-optimistic.etherscan.io/api",
  syscoin: "",
};

const blockScoutChainsTimeout: Set<Chain> = new Set(["fuse", "metis", "celo", "emerald"]);
const harmonyRpcChains: Set<Chain> = new Set(["harmony"]);

async function getCreationBlockFromExplorer(contractAddress: string, explorerUrl: string) {
  var url =
    explorerUrl +
    `?module=account&action=txlist&address=${contractAddress}&startblock=1&endblock=999999999&page=1&offset=1&sort=asc&limit=1`;
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, url } });

  const resp = await axios.get(url);

  if (!isArray(resp.data.result) || resp.data.result.length === 0) {
    logger.error({ msg: "No contract creation transaction found", data: { contractAddress, url, data: resp.data } });
    throw new Error("No contract creation transaction found");
  }
  const blockNumber: number = resp.data.result[0].blockNumber;
  const timestamp: number = resp.data.result[0].timeStamp;

  return { blockNumber, datetime: new Date(timestamp) };
}

async function getCreationTimestampBlockScoutScraping(contractAddress: string, explorerUrl: string, chain: Chain) {
  var url = explorerUrl + `/address/${contractAddress}`;
  logger.trace({ msg: "Fetching contract creation block", data: { contractAddress, url } });

  let resp = await axios.get(url);

  let tx = resp.data.split(`<a data-test="transaction_hash_link" href="/`)[1].split(`"`)[0];

  let txResp = await axios.get(`${explorerUrl}/${tx}/internal-transactions`);

  let blockNumber: number = txResp.data.split(`<a class="transaction__link" href="/block/`)[1].split(`"`)[0];

  const provider = new ethers.providers.JsonRpcProvider(sample(RPC_URLS[chain]));
  const block = await provider.getBlock(blockNumber);
  let timestamp = block.timestamp;

  return { blockNumber, datetime: new Date(timestamp) };
}

async function getCreationTimestampHarmonyRpc(contractAddress: string, chain: Chain) {
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

  if (
    !resp.data ||
    resp.data.id !== 1 ||
    !resp.data.result ||
    !resp.data.result.transactions ||
    resp.data.result.transactions.length !== 1
  ) {
    logger.error({
      msg: "Error while fetching contract creation block: Malformed response",
      data: { contractAddress, chain },
    });
    logger.error(resp.data);
    throw new Error("Malformed response");
  }

  const tx0 = resp.data.result.transactions[0];
  return { blockNumber: tx0.blockNulber, datetime: new Date(tx0.timestamp) };
}
