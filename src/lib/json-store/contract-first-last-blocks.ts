import { logger } from "../../utils/logger";
import { Chain, ContractBlockInfos } from "../../types/chain";
import * as ethers from "ethers";
import { callLockProtectedExplorerUrl } from "../shared-resources/shared-explorer";
import * as lodash from "lodash";
import axios from "axios";
import { callLockProtectedRpc } from "../shared-resources/shared-rpc";
import { SamplingPeriod } from "../../types/sampling";
import { streamBifiVaultOwnershipTransferedEventsFromRpc } from "../streamContractEventsFromRpc";
import { blockSamplesStore } from "../csv-store/csv-block-samples";
import { LocalFileStore } from "../../utils/local-file-store";
import * as path from "path";
import { DATA_DIRECTORY, shouldUseExplorer } from "../../utils/config";
import { normalizeAddress } from "../../utils/ethers";
import { cacheAsyncResultInRedis } from "../../utils/cache";

// store contract creation as a file as it never changes
export const contractCreationStore = new LocalFileStore({
  loggerScope: "ContractCreationStore",
  doFetch: async (chain: Chain, contractAddress: string): Promise<ContractBlockInfos> => {
    const useExplorer = shouldUseExplorer(chain);
    if (useExplorer) {
      return fetchContractFirstLastTrxFromExplorer(chain, contractAddress, "first");
    } else {
      const creationInfos = await getContractCreationInfosFromRPC(chain, contractAddress, "4hour");
      if (!creationInfos) {
        throw new Error(`Could not find contract creation block for ${contractAddress} on ${chain}`);
      }
      return creationInfos;
    }
  },
  format: "json",
  getLocalPath: (chain: Chain, contractAddress: string) =>
    path.join(DATA_DIRECTORY, "chain", chain, "contracts", normalizeAddress(contractAddress), "creation_date.json"),
  getResourceId: (chain: Chain, contractAddress: string) => `${chain}:${contractAddress}:creation_date`,
  ttl_ms: null, // never expires,
  retryOnFetchError: false, // work function already implement retry
});

// store last transaction in redis as it changes frequently
export const contractLastTrxStore = {
  fetchData: cacheAsyncResultInRedis(
    async (chain: Chain, contractAddress: string): Promise<ContractBlockInfos> => {
      return fetchContractFirstLastTrxFromExplorer(chain, contractAddress, "last");
    },
    {
      getKey: (chain: Chain, contractAddress: string) => `${chain}:${contractAddress}:last-trx`,
      dateFields: ["datetime"],
      ttl_sec: 60 * 60, // 1 hour
    }
  ),
};

async function fetchContractFirstLastTrxFromExplorer(
  chain: Chain,
  contractAddress: string,
  type: "first" | "last"
): Promise<ContractBlockInfos> {
  logger.debug(`Fetching ${type} trx for ${chain}:${contractAddress}`);
  // special case for harmony where the explorer api is different
  if (chain === "harmony") {
    return getCreationTimestampHarmonyRpc(chain, contractAddress, type);
  }

  const sort = type === "first" ? "asc" : "desc";
  const params: Record<string, string> = {
    module: "account",
    action: "txlist",
    address: contractAddress,
    sort: sort,
    page: "1",
    offset: "1",
    limit: "1", // mostly ignored, but just in case
  };
  // for some reason, fuse rpc explorer fails with too small pages
  // KO (504): https://explorer.fuse.io/api?module=account&action=txlist&address=0x641Ec255eD35C7bf520745b6E40E6f3d989D0ff2&sort=asc&page=1&offset=1&limit=1
  // OK : https://explorer.fuse.io/api?module=account&action=txlist&address=0x641Ec255eD35C7bf520745b6E40E6f3d989D0ff2&sort=asc&page=1&offset=100&limit=1
  if (chain === "fuse") {
    params.offset = "100";
  }
  const creationRes = await callLockProtectedExplorerUrl<any>(chain, params);
  const trxInfos = creationRes[0];
  const block = parseInt(trxInfos.blockNumber);
  const timestamp = trxInfos.timeStamp;
  const trxHash = trxInfos.hash;

  logger.debug(`[CTI] ${type} block for ${chain}:${contractAddress}: ${block} - timestamp: ${timestamp}`);
  return {
    blockNumber: block,
    datetime: new Date(timestamp * 1000),
    transactionHash: trxHash,
    chain,
    contractAddress: ethers.utils.getAddress(contractAddress),
  };
}

async function getContractCreationInfosFromRPC(
  chain: Chain,
  contractAddress: string,
  samplingPeriodForFirstBlock: SamplingPeriod
): Promise<ContractBlockInfos | null> {
  const firstBlock = await blockSamplesStore.getFirstRow(chain, samplingPeriodForFirstBlock);
  if (!firstBlock) {
    logger.info(`[CTI] No blocks samples imported yet.`);
    return null;
  }
  logger.info(
    `[CTI] Fetching contract creation for ${chain}:${contractAddress} from block: ${JSON.stringify(firstBlock)}`
  );
  const eventStream = streamBifiVaultOwnershipTransferedEventsFromRpc(chain, contractAddress, firstBlock.blockNumber);

  for await (const event of eventStream) {
    return {
      chain,
      contractAddress,
      blockNumber: event.blockNumber,
      datetime: event.datetime,
      transactionHash: event.transactionHash,
    };
  }
  return null;
}

async function getCreationTimestampHarmonyRpc(
  chain: Chain,
  contractAddress: string,
  type: "first" | "last"
): Promise<ContractBlockInfos> {
  logger.debug(`Fetching harmony timestamp ${chain}:${contractAddress}:${type}`);
  const resp = await callLockProtectedRpc(chain, async (provider) => {
    const url = provider.connection.url;
    return await axios.post(url, {
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
  });
  const transaction = lodash.get(resp, "data.result.transactions[0]") as
    | undefined
    | { blockNumber: number; timestamp: number; hash: string };
  if (!transaction) {
    throw new Error(`No transaction found for ${chain}:${contractAddress}: ${JSON.stringify(resp)}`);
  }

  return {
    blockNumber: transaction.blockNumber,
    datetime: new Date(transaction.timestamp * 1000),
    chain,
    contractAddress,
    transactionHash: transaction.hash,
  };
}
