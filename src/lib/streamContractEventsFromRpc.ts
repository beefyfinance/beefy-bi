import { Chain } from "../types/chain";
import {
  getFirstTransactionInfos,
  getLastTransactionInfos,
} from "./contract-transaction-infos";
import { getContract } from "../utils/ethers";
import { logger } from "../utils/logger";
import * as lodash from "lodash";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { ethers } from "ethers";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, LOG_LEVEL } from "../utils/config";
import { backOff } from "exponential-backoff";

async function* streamContractEventsFromRpc<TEventArgs>(
  chain: Chain,
  contractAddress: string,
  abi: ethers.ContractInterface,
  eventName: string,
  options?: {
    startBlock?: number;
    endBlock?: number;
    blockBatchSize?: number;
    mapArgs?: (args: ethers.utils.Result) => TEventArgs;
    getEventFilters?: (
      filters: ethers.BaseContract["filters"]
    ) => ethers.EventFilter;
    timeOrder?: "timeline" | "reverse";
  }
) {
  let startBlock = options?.startBlock;
  if (!startBlock) {
    const { blockNumber } = await getFirstTransactionInfos(
      chain,
      contractAddress
    );
    startBlock = blockNumber;
  }
  let endBlock = options?.endBlock;
  if (!endBlock) {
    const { blockNumber } = await getLastTransactionInfos(
      chain,
      contractAddress
    );
    endBlock = blockNumber;
  }

  // we will need to call the contract to get the ppfs at some point
  const mapArgs = options?.mapArgs || ((x) => x as any as TEventArgs);

  // iterate through block ranges
  const rangeSize =
    options?.blockBatchSize || CHAIN_RPC_MAX_QUERY_BLOCKS[chain]; // big to speed up, not to big to avoid rpc limitations
  const flat_range = lodash.range(startBlock, endBlock + 1, rangeSize);
  flat_range.push(endBlock + 1); // to make sure we get the last block
  let ranges: { fromBlock: number; toBlock: number }[] = [];
  for (let i = 0; i < flat_range.length - 1; i++) {
    ranges.push({
      fromBlock: flat_range[i],
      toBlock: flat_range[i + 1] - 1,
    });
  }
  if (options?.timeOrder === "reverse") {
    ranges = ranges.reverse();
  }
  logger.verbose(
    `[ERC20.T.RPC] Iterating through ${ranges.length} ranges for ${chain}:${contractAddress}:${eventName}`
  );
  for (const [rangeIdx, blockRange] of ranges.entries()) {
    const events = await backOff(
      () => {
        // instanciate contract late to shuffle rpcs on error
        const contract = getContract(chain, abi, contractAddress);
        const eventFilter = options?.getEventFilters
          ? options?.getEventFilters(contract.filters)
          : contract.filters[eventName]();
        return contract.queryFilter(
          eventFilter,
          blockRange.fromBlock,
          blockRange.toBlock
        );
      },
      {
        retry: async (error, attemptNumber) => {
          logger.error(
            `[ERC20.T.RPC] Error on attempt ${attemptNumber} for ${chain}:${contractAddress}:${eventName} (${blockRange.fromBlock}->${blockRange.toBlock}) : ${error}`
          );
          if (LOG_LEVEL === "trace") {
            console.error(error);
          }
          return true;
        },
        numOfAttempts: 10,
        startingDelay: 1000,
        delayFirstAttempt: true,
      }
    );

    if (events.length > 0) {
      logger.verbose(
        `[ERC20.T.RPC] Got ${events.length} events for range ${rangeIdx}/${ranges.length}`
      );
    } else {
      logger.debug(
        `[ERC20.T.RPC] No events for range ${rangeIdx}/${ranges.length}`
      );
    }

    for (const rawEvent of events) {
      if (!rawEvent.args) {
        throw new Error(`No event args in event ${rawEvent}`);
      }
      const mappedEvent = {
        blockNumber: rawEvent.blockNumber,
        datetime: new Date((await rawEvent.getBlock()).timestamp),
        data: mapArgs(rawEvent.args),
      };
      yield mappedEvent;
    }
  }
}

export const streamERC20TransferEventsFromRpc = (
  chain: Chain,
  contractAddress: string,
  options?: {
    from?: string;
    to?: string;
    startBlock?: number;
    endBlock?: number;
    blockBatchSize?: number;
    timeOrder?: "timeline" | "reverse";
  }
) => {
  logger.debug(
    `[ERC20.T.RPC] Streaming ERC20 transfer events for ${chain}:${contractAddress} ${JSON.stringify(
      options
    )}`
  );
  return streamContractEventsFromRpc<{
    from: string;
    to: string;
    value: string;
  }>(chain, contractAddress, ERC20Abi, "Transfer", {
    getEventFilters: (filters) => {
      if (options?.from && options?.to) {
        return filters.Transfer(options.from, options.to);
      } else if (options?.from) {
        return filters.Transfer(options.from, null);
      } else if (options?.to) {
        return filters.Transfer(null, options.to);
      } else {
        return filters.Transfer();
      }
    },
    mapArgs: (args) => ({
      from: args.from,
      to: args.to,
      value: args.value.toString(),
    }),
    startBlock: options?.startBlock,
    endBlock: options?.endBlock,
    blockBatchSize: options?.blockBatchSize,
    timeOrder: options?.timeOrder,
  });
};

export async function* streamBifiVaultUpgradeStratEventsFromRpc(
  chain: Chain,
  contractAddress: string
) {
  // add a fake event for the contract creation
  const { blockNumber: deployBlockNumber, datetime: deployBlockDatetime } =
    await getFirstTransactionInfos(chain, contractAddress);
  const contract = getContract(chain, BeefyVaultV6Abi, contractAddress);
  const firstStrategyRes = await contract.functions.strategy({
    blockTag: deployBlockNumber,
  });
  yield {
    blockNumber: deployBlockNumber,
    datetime: deployBlockDatetime,
    data: { implementation: firstStrategyRes[0] as string },
  };
  // add a shortcut if the strategy never changed
  const currentStrategyRes = await contract.functions.strategy();
  if (firstStrategyRes[0] === currentStrategyRes[0]) {
    logger.verbose(
      `[ERC20.T.RPC] Shortcut: no strategy change events for ${chain}:${contractAddress}`
    );
    return;
  }

  const eventStream = streamContractEventsFromRpc<{ implementation: string }>(
    chain,
    contractAddress,
    BeefyVaultV6Abi,
    "UpgradeStrat",
    {
      mapArgs: (args) => ({
        implementation: args.implementation,
      }),
    }
  );
  // just iteration to the event stream
  yield* eventStream;
}
