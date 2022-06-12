import { logger } from "../utils/logger";
import _ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { ERC20EventData } from "../lib/csv-transfer-events";
import * as lodash from "lodash";
import { Chain } from "../types/chain";
import { callLockProtectedExplorerUrl } from "./shared-explorer";

const ERC20Abi = _ERC20Abi as any as JsonAbi;

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
  topics: string[];
  data: string;
  blockNumber: number;
  timeStamp: number;
  gasPrice: number;
  gasUsed: number;
  logIndex: number;
  transactionHash: string;
  transactionIndex: number;
}

// be nice to explorers or you'll get banned
async function fetchExplorerLogsPage<TRes extends { blockNumber: number }>(
  chain: Chain,
  contractAddress: string,
  abi: JsonAbi,
  eventName: string,
  fromBlock: number,
  formatEvent: (event: ExplorerLog) => TRes
) {
  logger.debug(
    `[ERC20.T.EX] Fetching ${eventName} events from ${fromBlock} for ${chain}:${contractAddress}`
  );
  const eventTopic = getEventTopicFromJsonAbi(abi, eventName);
  const rawLogs = await callLockProtectedExplorerUrl<ExplorerLog[]>(chain, {
    module: "logs",
    action: "getLogs",
    address: contractAddress,
    topic0: eventTopic,
    fromBlock: fromBlock.toString(),
  });
  let logs = rawLogs.map(formatEvent);
  const mayHaveMore = logs.length === 1000;

  logger.verbose(
    `[ERC20.T.EX] Got ${logs.length} ${eventName} events for ${chain}:${contractAddress}, mayHaveMore: ${mayHaveMore}`
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

function explorerLogToERC20TransferEvent(event: ExplorerLog): ERC20EventData {
  const blockNumber = parseInt(
    ethers.BigNumber.from(event.blockNumber).toString()
  );
  const data =
    "0x" +
    event.topics
      .slice(1)
      .concat([event.data])
      .map((hexData: string) => hexData.slice(2))
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
  fromBlock: number
) {
  let mayHaveMore = true;
  while (mayHaveMore) {
    const pageRes = await fetchExplorerLogsPage(
      chain,
      contractAddress,
      ERC20Abi,
      "Transfer",
      fromBlock,
      explorerLogToERC20TransferEvent
    );
    if (pageRes.logs.length === 0) {
      return;
    }
    yield* pageRes.logs;
    mayHaveMore = pageRes.mayHaveMore;
    fromBlock = pageRes.logs[pageRes.logs.length - 1].blockNumber + 1;
  }
}
