import axios from "axios";
import { ethers } from "ethers";
import { sample } from "lodash";
import { Chain } from "../../../types/chain";
import { RPC_URLS } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import * as Rx from "rxjs";
import { batchQueryGroup$ } from "../../../utils/rxjs/utils/batch-query-group";

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
  formatOutput: (obj: TObj, blockDate: { blockNumber: number; datetime: Date }) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchQueryGroup$({
    bufferCount: 5,
    toQueryObj: (obj: TObj[]) => options.getCallParams(obj[0]),
    getBatchKey: (obj: TObj) => {
      const params = options.getCallParams(obj);
      return `${params.chain}-${params.contractAddress}`;
    },
    // do the actual processing
    processBatch: async (params: TParams[]) => {
      const results: ContractCreationInfos[] = [];

      for (const param of params) {
        const result = await getContractCreationInfos(param.contractAddress, param.chain);
        results.push(result);
      }

      return results;
    },
    formatOutput: options.formatOutput,
  });
}

async function getContractCreationInfos(contractAddress: string, chain: Chain) {
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

const blockScoutChainsTimeout = new Set(["fuse", "metis", "celo", "emerald"]);
const harmonyRpcChains = new Set(["one"]);

async function getCreationBlockFromExplorer(contractAddress: string, explorerUrl: string) {
  var url =
    explorerUrl +
    `?module=account&action=txlist&address=${contractAddress}&startblock=1&endblock=99999999&page=1&offset=1&sort=asc&limit=1`;
  const resp = await axios.get(url);

  console.dir(resp.data, { depth: null });
  const blockNumber: string = resp.data.result[0].blockNumber;
  const timestamp: number = resp.data.result[0].timeStamp;

  return { blockNumber, datetime: new Date(timestamp) };
}

async function getCreationTimestampBlockScoutScraping(contractAddress: string, explorerUrl: string, chain: Chain) {
  var url = explorerUrl + `/address/${contractAddress}`;

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
  const resp = await axios.post(rpcUrl, {
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
  });

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
