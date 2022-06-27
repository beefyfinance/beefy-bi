import { logger } from "../utils/logger";
import _ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import _BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { ethers } from "ethers";
import { ERC20EventData } from "../lib/csv-transfer-events";
import * as lodash from "lodash";
import { Chain } from "../types/chain";
import { callLockProtectedExplorerUrl } from "./shared-resources/shared-explorer";
import { callLockProtectedRpc } from "./shared-resources/shared-rpc";
import { fetchContractCreationInfos } from "./fetch-if-not-found-locally";

const ERC20Abi = _ERC20Abi as any as JsonAbi;
const BeefyVaultV6Abi = _BeefyVaultV6Abi as any as JsonAbi;

type JsonAbi = {
  inputs?: {
    internalType: string;
    name: string;
    type: string;
    indexed?: boolean;
  }[];
  stateMutability?: string;
  type: string;
  anonymous?: boolean;
  name?: string;
  outputs?: {
    internalType: string;
    name: string;
    type: string;
  }[];
}[];

interface ExplorerLog {
  address: string;
  topics: (string | null)[];
  data: string;
  blockNumber: number | string;
  timeStamp: number | string;
  gasPrice: number | string;
  gasUsed: number | string;
  logIndex: number | string;
  transactionHash: string;
  transactionIndex: number | string;
}

// be nice to explorers or you'll get banned
async function fetchExplorerLogsPage<TRes extends { blockNumber: number }>(
  chain: Chain,
  contractAddress: string,
  abi: JsonAbi,
  eventName: string,
  fromBlock: number,
  toBlock: number | null,
  formatEvent: (event: ExplorerLog) => TRes,
  fromAddress?: string
) {
  logger.debug(
    `[ERC20.T.EX] Fetching ${eventName} events from ${fromBlock} for ${chain}:${contractAddress}:${fromAddress}`
  );
  const eventTopic = getEventTopicFromJsonAbi(abi, eventName);
  const params: Record<string, string> = {
    module: "logs",
    action: "getLogs",
    address: contractAddress,
    topic0: eventTopic,
    fromBlock: fromBlock.toString(),
  };
  let didLimitBlockCount = false;
  if (toBlock) {
    params.toBlock = toBlock.toString();
    const blockCount = toBlock - fromBlock;
    if (blockCount > 10000) {
      logger.info(
        `[ERC20.T.EX] Limiting block count to 10k blocks to avoid 504s from explorers`
      );
      params.toBlock = (fromBlock + 10000).toString();
      didLimitBlockCount = true;
    }
  }
  if (fromAddress) {
    params.topic1 =
      "0x000000000000000000000000" + fromAddress.slice(2) /** remove "0x" */;
  }
  if (chain === "metis") {
    // https://andromeda-explorer.metis.io/api-docs
    // Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: topic0_1_opr","result":null,"status":"0"}
    // we use "or" because there is no topic 1
    params.topic0_1_opr = "or";
  }
  const rawLogs = await callLockProtectedExplorerUrl<ExplorerLog[]>(
    chain,
    params
  );
  let logs = rawLogs.map(formatEvent);
  const mayHaveMore = didLimitBlockCount ? true : logs.length === 1000;

  logger.verbose(
    `[ERC20.T.EX] Got ${logs.length} ${eventName} events for ${chain}:${contractAddress}:${fromAddress}, mayHaveMore: ${mayHaveMore}`
  );
  // remove last block data as the explorer may have truncated results
  // in the middle of a block
  if (mayHaveMore) {
    const lastLogBlock = logs[logs.length - 1];
    for (let i = logs.length - 1; i >= 0; i--) {
      if (logs[i].blockNumber === lastLogBlock.blockNumber) {
        logs.pop();
      } else {
        break;
      }
    }
  }
  return { logs, mayHaveMore };
}

const getEventTopicFromJsonAbi = lodash.memoize(
  function _getEventTopicFromJsonAbi(abi: JsonAbi, eventName: string) {
    const eventTypes = getEventTypesFromJsonAbi(abi, eventName);
    return ethers.utils.keccak256(
      ethers.utils.toUtf8Bytes(`${eventName}(${eventTypes.join(",")})`)
    );
  }
);

const getEventTypesFromJsonAbi = lodash.memoize(
  function _getEventTypesFromJsonAbi(abi: JsonAbi, eventName: string) {
    const eventConfig = abi.find(
      (abi) => abi.name === eventName && abi.type === "event"
    );
    if (!eventConfig || !eventConfig.inputs) {
      throw new Error(`${eventName} not found in abi`);
    }
    return eventConfig.inputs.map((input) => input.type);
  }
);

export function explorerLogToERC20TransferEvent(
  event: ExplorerLog
): ERC20EventData {
  const blockNumber = parseInt(
    ethers.BigNumber.from(event.blockNumber).toString()
  );
  const data =
    "0x" +
    event.topics
      .slice(1)
      .concat([event.data])
      .map((hexData: string | null) => (hexData ? hexData.slice(2) : hexData))
      .join("");
  const [from, to, value] = ethers.utils.defaultAbiCoder.decode(
    getEventTypesFromJsonAbi(ERC20Abi, "Transfer"),
    data
  );
  const datetime = new Date(
    ethers.BigNumber.from(event.timeStamp).toNumber() * 1000
  );
  return {
    blockNumber,
    datetime,
    from,
    to,
    value: value.toString(),
  };
}

export async function* streamERC20TransferEventsFromExplorer(
  chain: Chain,
  contractAddress: string,
  fromBlock: number,
  toBlock: number | null,
  fromAddress?: string
) {
  let mayHaveMore = true;
  while (mayHaveMore) {
    const pageRes = await fetchExplorerLogsPage(
      chain,
      contractAddress,
      ERC20Abi,
      "Transfer",
      fromBlock,
      toBlock,
      explorerLogToERC20TransferEvent,
      fromAddress
    );
    if (pageRes.logs.length === 0) {
      return;
    }
    yield* pageRes.logs;
    mayHaveMore = pageRes.mayHaveMore;
    fromBlock = pageRes.logs[pageRes.logs.length - 1].blockNumber + 1;
  }
}

interface BeefyVaultV6StrategyUpgradeEvent {
  blockNumber: number;
  datetime: Date;
  data: { implementation: string };
}

function explorerLogToBeefyVaultV6UpgradeStratEvent(
  event: ExplorerLog
): BeefyVaultV6StrategyUpgradeEvent {
  const blockNumber = parseInt(
    ethers.BigNumber.from(event.blockNumber).toString()
  );
  const data =
    "0x" +
    event.topics
      .slice(1)
      .concat([event.data])
      .map((hexData: string | null) => (hexData ? hexData.slice(2) : hexData))
      .join("");
  const [implementation] = ethers.utils.defaultAbiCoder.decode(
    getEventTypesFromJsonAbi(BeefyVaultV6Abi, "UpgradeStrat"),
    data
  );
  const datetime = new Date(
    ethers.BigNumber.from(event.timeStamp).toNumber() * 1000
  );
  return {
    blockNumber,
    datetime,
    data: {
      implementation,
    },
  };
}

export async function* streamBifiVaultUpgradeStratEventsFromExplorer(
  chain: Chain,
  contractAddress: string,
  startBlock: number,
  endBlock: number | null
) {
  const { blockNumber: deployBlockNumber, datetime: deployBlockDatetime } =
    await fetchContractCreationInfos(chain, contractAddress);

  // first, get all strategy upgrade events
  // it is very unlikely that there are more than 1000 events
  let allStrategyEvents: BeefyVaultV6StrategyUpgradeEvent[] = [];
  let mayHaveMore = true;
  let fromBlock =
    startBlock > deployBlockNumber ? startBlock : deployBlockNumber;
  while (mayHaveMore) {
    const pageRes = await fetchExplorerLogsPage(
      chain,
      contractAddress,
      BeefyVaultV6Abi,
      "UpgradeStrat",
      fromBlock,
      endBlock,
      explorerLogToBeefyVaultV6UpgradeStratEvent
    );
    allStrategyEvents = allStrategyEvents.concat(pageRes.logs);
    mayHaveMore = pageRes.mayHaveMore;
    if (pageRes.logs.length > 0) {
      fromBlock = pageRes.logs[pageRes.logs.length - 1].blockNumber + 1;
    } else {
      break;
    }
  }

  // now, we want to yield a fake event for the strategy on deploy
  // if the stragegy has never been upgraded, yield the current strategy at the deploy block time
  // if the stragegy has been upgraded, yield the strategy at the block preceeding the first upgrade block time

  let callOptions: { blockTag: number } | undefined = undefined;
  if (allStrategyEvents.length === 0) {
    callOptions = undefined;
  } else {
    const firstUpgradeBlockNumber = allStrategyEvents[0].blockNumber;
    callOptions = { blockTag: firstUpgradeBlockNumber - 1 };
  }
  logger.debug(
    `[PPFS] Fetching strategy implem for ${chain}:${contractAddress} (${JSON.stringify(
      callOptions
    )})`
  );
  const strategyImplem = await callLockProtectedRpc(chain, async (provider) => {
    const contract = new ethers.Contract(
      contractAddress,
      BeefyVaultV6Abi,
      provider
    );
    const [stragegy] =
      callOptions === undefined
        ? await contract.functions.strategy()
        : await contract.functions.strategy(callOptions);
    return stragegy;
  });
  // yield the strategy at the deploy block time
  yield {
    blockNumber: deployBlockNumber,
    datetime: deployBlockDatetime,
    data: {
      implementation: strategyImplem,
    },
  };
  // then yield the rest of our data
  yield* allStrategyEvents;
}
