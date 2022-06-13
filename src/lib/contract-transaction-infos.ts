import { logger } from "../utils/logger";
import { cacheAsyncResult } from "../utils/cache";
import { Chain } from "../types/chain";
import * as ethers from "ethers";
import { callLockProtectedExplorerUrl } from "./shared-explorer";
import * as lodash from "lodash";
import { RPC_URLS } from "../utils/config";
import axios from "axios";

interface ContractCreationInfo {
  chain: Chain;
  contractAddress: string;
  transactionHash: string;
  blockNumber: number;
  datetime: Date;
}

export async function _fetchContractFirstLastTrx(
  chain: Chain,
  contractAddress: string,
  type: "first" | "last"
): Promise<ContractCreationInfo> {
  logger.debug(`Fetching ${type} trx for ${chain}:${contractAddress}`);
  // special case for harmony where the explorer api is different
  if (chain === "harmony") {
    return getCreationTimestampHarmonyRpc(chain, contractAddress, type);
  }

  const sort = type === "first" ? "asc" : "desc";

  const creationRes = await callLockProtectedExplorerUrl<any>(chain, {
    module: "account",
    action: "txlist",
    address: contractAddress,
    sort: sort,
    limit: "1",
  });

  const trxInfos = creationRes[0];
  const block = parseInt(trxInfos.blockNumber);
  const timestamp = trxInfos.timeStamp;
  const trxHash = trxInfos.hash;

  logger.debug(
    `${type} block for ${chain}:${contractAddress}: ${block} - timestamp: ${timestamp}`
  );
  return {
    blockNumber: block,
    datetime: new Date(timestamp * 1000),
    transactionHash: trxHash,
    chain,
    contractAddress: ethers.utils.getAddress(contractAddress),
  };
}

export const getFirstTransactionInfos = cacheAsyncResult(
  function fetchFirstTrxInfos(chain: Chain, contractAddress: string) {
    return _fetchContractFirstLastTrx(chain, contractAddress, "first");
  },
  {
    getKey: (chain, contractAddress) => `${chain}:${contractAddress}:first`,
    dateFields: ["datetime"],
  }
);

export const getLastTransactionInfos = cacheAsyncResult(
  function fetchFirstTrxInfos(chain: Chain, contractAddress: string) {
    return _fetchContractFirstLastTrx(chain, contractAddress, "last");
  },
  {
    getKey: (chain, contractAddress) => `${chain}:${contractAddress}:last`,
    dateFields: ["datetime"],
    ttl_sec: 60 * 60, // 1h
  }
);

async function getCreationTimestampHarmonyRpc(
  chain: Chain,
  contractAddress: string,
  type: "first" | "last"
): Promise<ContractCreationInfo> {
  const url = lodash.sample(RPC_URLS[chain]) as string;
  const resp = await axios.post(url, {
    jsonrpc: "2.0",
    method: "hmyv2_getTransactionsHistory",
    params: [
      {
        address: contractAddress,
        pageIndex: 0,
        pageSize: 1,
        fullTx: true,
        txType: "ALL",
        order: type === "first" ? "ASC" : "DESC",
      },
    ],
    id: 1,
  });
  const transaction = lodash.get(resp, "data.result.transactions[0]") as
    | undefined
    | { blockNumber: number; timestamp: number; hash: string };
  if (!transaction) {
    throw new Error(
      `No transaction found for ${chain}:${contractAddress}: ${JSON.stringify(
        resp
      )}`
    );
  }

  return {
    blockNumber: transaction.blockNumber,
    datetime: new Date(transaction.timestamp * 1000),
    chain,
    contractAddress,
    transactionHash: transaction.hash,
  };
}
