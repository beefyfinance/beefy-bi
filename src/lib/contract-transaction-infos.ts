import axios from "axios";
import { logger } from "../utils/logger";
import { cacheAsyncResult } from "../utils/cache";
import { Chain } from "../types/chain";
import * as ethers from "ethers";
import { EXPLORER_URLS } from "../utils/config";
import * as lodash from "lodash";
import { sleep } from "../utils/async";

interface ContractCreationInfo {
  chain: Chain;
  contractAddress: string;
  transactionHash: string;
  blockNumber: number;
  datetime: Date;
}

let lastCallDate = new Date(0);
let callMaxEvery = 1000 * 5.5; // 1 req per 5 sec max

export async function _fetchContractFirstLastTrx(
  chain: Chain,
  contractAddress: string,
  type: "first" | "last"
): Promise<ContractCreationInfo> {
  logger.debug(`Fetching ${type} trx for ${chain}:${contractAddress}`);
  const explorerUrl = EXPLORER_URLS[chain];
  const sort = type === "first" ? "asc" : "desc";
  var url =
    explorerUrl +
    `?module=account&action=txlist&address=${contractAddress}&startblock=1&endblock=99999999&page=1&offset=1&sort=${sort}&limit=1`;

  // avoid hitting the explorer too often
  if (new Date().getTime() - lastCallDate.getTime() < callMaxEvery) {
    const sleepFor = Math.min(
      callMaxEvery,
      callMaxEvery - (new Date().getTime() - lastCallDate.getTime())
    );
    logger.verbose(`Sleeping for ${sleepFor}ms`);
    await sleep(sleepFor);
  }
  const resp = await axios.get(url);
  lastCallDate = new Date();

  const creationRes = resp.data?.result;
  if (!creationRes || !lodash.isArray(creationRes) || !creationRes.length) {
    throw new Error(
      `${type} trx for ${chain}:${contractAddress} not found: ${resp.data?.result}`
    );
  }

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
