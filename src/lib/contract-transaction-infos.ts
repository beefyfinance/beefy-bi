import { logger } from "../utils/logger";
import { Chain } from "../types/chain";
import * as ethers from "ethers";
import { callLockProtectedExplorerUrl } from "./shared-resources/shared-explorer";
import * as lodash from "lodash";
import axios from "axios";
import { callLockProtectedRpc } from "./shared-resources/shared-rpc";
import { SamplingPeriod } from "../types/sampling";
import { streamBifiVaultOwnershipTransferedEventsFromRpc } from "./streamContractEventsFromRpc";
import { blockSamplesStore } from "./csv-block-samples";

export interface ContractCreationInfo {
  chain: Chain;
  contractAddress: string;
  transactionHash: string;
  blockNumber: number;
  datetime: Date;
}

export async function fetchContractFirstLastTrxFromExplorer(
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

  logger.debug(
    `[CTI] ${type} block for ${chain}:${contractAddress}: ${block} - timestamp: ${timestamp}`
  );
  return {
    blockNumber: block,
    datetime: new Date(timestamp * 1000),
    transactionHash: trxHash,
    chain,
    contractAddress: ethers.utils.getAddress(contractAddress),
  };
}

export async function getContractCreationInfosFromRPC(
  chain: Chain,
  contractAddress: string,
  samplingPeriodForFirstBlock: SamplingPeriod
): Promise<ContractCreationInfo | null> {
  const firstBlock = await blockSamplesStore.getFirstRow(
    chain,
    samplingPeriodForFirstBlock
  );
  if (!firstBlock) {
    logger.info(`[CTI] No blocks samples imported yet.`);
    return null;
  }
  logger.info(
    `[CTI] Fetching contract creation for ${chain}:${contractAddress} from block: ${JSON.stringify(
      firstBlock
    )}`
  );
  const eventStream = streamBifiVaultOwnershipTransferedEventsFromRpc(
    chain,
    contractAddress,
    firstBlock.blockNumber
  );

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
): Promise<ContractCreationInfo> {
  logger.debug(
    `Fetching harmony timestamp ${chain}:${contractAddress}:${type}`
  );
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
