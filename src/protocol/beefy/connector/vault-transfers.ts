import * as lodash from "lodash";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import { Chain } from "../../../types/chain";
import { flatten, zipWith } from "lodash";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

export async function fetchBeefyVaultV6InvestmentChanges(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractAddresses: string[],
  fromBlock: number,
  toBlock: number
): Promise<{ contractAddress: string; blockNumber: number; from: string; to: string; value: string }[]> {
  logger.debug({
    msg: "Fetching withdraw and deposits for vault",
    data: { chain, contractAddresses, fromBlock, toBlock },
  });

  // fetch all contract logs in one call
  const eventsPromises: Promise<ethers.Event[]>[] = [];
  for (const contractAddress of contractAddresses) {
    const contract = new ethers.Contract(contractAddress, ERC20Abi, provider);
    const eventFilter = contract.filters.Transfer(fromBlock, toBlock);
    const eventsPromise = contract.queryFilter(eventFilter, fromBlock, toBlock);
    eventsPromises.push(eventsPromise);
  }
  const eventsRes = await Promise.all(eventsPromises);

  const events = flatten(
    zipWith(contractAddresses, eventsRes, (contractAddress, events) =>
      events.map((event) => ({
        contractAddress,
        args: event.args,
        blockNumber: event.blockNumber,
        transactionHash: event.transactionHash,
      }))
    )
  );

  // shortcut if we have no events for this batch
  if (events.length === 0) {
    logger.debug({ msg: "Got no events for range", data: { chain, contractAddresses, fromBlock, toBlock } });
    return [];
  } else {
    logger.debug({
      msg: "Got events for range",
      data: { chain, contractAddresses, fromBlock, toBlock, total: events.length },
    });
  }

  // now we get all blocks in one go
  const blockNumbers = lodash.uniq(events.map((event) => event.blockNumber));
  const blocks = await Promise.all(
    blockNumbers.map((blockNumber) => {
      return provider.getBlock(blockNumber);
    })
  );

  logger.debug({ msg: "Fetched blocks", data: { total: blocks.length, chain, contractAddresses } });
  const blockByNumber = lodash.keyBy(blocks, "number");

  return events.map((event) => {
    if (!event.args) {
      throw new Error(`No event args in event ${event}`);
    }
    return {
      contractAddress: event.contractAddress,
      transactionHash: event.transactionHash,
      blockNumber: event.blockNumber,
      datetime: new Date(blockByNumber[event.blockNumber].timestamp * 1000),
      from: event.args.from,
      to: event.args.to,
      value: event.args.value,
    };
  });
}
