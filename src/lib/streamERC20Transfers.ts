import { Chain } from "../types/chain";
import {
  getFirstTransactionInfos,
  getLastTransactionInfos,
} from "./contract-transaction-infos";
import { getContract } from "../utils/ethers";
import { logger } from "../utils/logger";
import * as lodash from "lodash";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";

interface TransferEvent {
  blockNumber: number;
  datetime: Date;
  from: string;
  to: string;
  value: ethers.BigNumber;
}

export async function* streamERC20TransferEvents(
  chain: Chain,
  contractAddress: string,
  options?: {
    startBlock?: number;
    endBlock?: number;
  }
) {
  const { blockNumber: createBlock, transactionHash: creationTrxHash } =
    await getFirstTransactionInfos(chain, contractAddress);
  const { blockNumber: lastBlock, transactionHash: lastTrxHash } =
    await getLastTransactionInfos(chain, contractAddress);

  const startBlock = options?.startBlock || createBlock;
  const endBlock = options?.endBlock || lastBlock;

  // we will need to call the contract to get the ppfs at some point
  const contract = getContract(chain, ERC20Abi, contractAddress);

  // iterate through block ranges
  const rangeSize = 3000; // big to speed up, not to big to avoid rpc limitations
  const flat_range = lodash.range(startBlock, endBlock + 1, rangeSize);
  const ranges: { fromBlock: number; toBlock: number }[] = [];
  for (let i = 0; i < flat_range.length - 1; i++) {
    ranges.push({
      fromBlock: flat_range[i],
      toBlock: flat_range[i + 1] - 1,
    });
  }
  logger.debug(
    `[ERC20_EVENT_STREAM] Iterating through ${ranges.length} ranges for ${chain}:${contractAddress}`
  );
  const eventFilter = contract.filters.Transfer();
  for (const [rangeIdx, blockRange] of ranges.entries()) {
    const events = await contract.queryFilter(
      eventFilter,
      blockRange.fromBlock,
      blockRange.toBlock
    );
    logger.verbose(
      `[ERC20_EVENT_STREAM] Got ${events.length} events for ${chain}:${contractAddress} range ${rangeIdx}/${ranges.length}`
    );

    let blockNum = 0;
    let blockDate = new Date();
    for (const rawEvent of events) {
      if (!rawEvent.args) {
        throw new Error(`No event args in event ${rawEvent}`);
      }
      if (blockNum !== rawEvent.blockNumber) {
        blockNum = rawEvent.blockNumber;
        const block = await rawEvent.getBlock();
        blockDate = new Date(block.timestamp * 1000);
      }
      const transferEvent: TransferEvent = {
        blockNumber: rawEvent.blockNumber,
        datetime: blockDate,
        from: rawEvent.args.from,
        to: rawEvent.args.to,
        value: rawEvent.args.value,
      };
      yield transferEvent;
    }
  }
}
