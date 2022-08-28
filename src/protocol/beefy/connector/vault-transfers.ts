import * as lodash from "lodash";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import { Chain } from "../../../types/chain";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

export async function fetchBeefyVaultV6InvestmentChanges(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractAddress: string,
  fromBlock: number,
  toBlock: number
): Promise<{ blockNumber: number; from: string; to: string; value: string }[]> {
  logger.debug({
    msg: "Fetching withdraw and deposits for vault",
    data: { chain, contractAddress, fromBlock, toBlock },
  });
  // instanciate contract late to shuffle rpcs on error
  const contract = new ethers.Contract(contractAddress, ERC20Abi, provider);
  const eventFilter = contract.filters.Transfer(fromBlock, toBlock);
  const events = await contract.queryFilter(eventFilter, fromBlock, toBlock);

  // shortcut if we have no events for this batch
  if (events.length === 0) {
    logger.debug({ msg: "Got no events for range", data: { chain, contractAddress, fromBlock, toBlock } });
    return [];
  } else {
    logger.debug({
      msg: "Got events for range",
      data: { chain, contractAddress, fromBlock, toBlock, total: events.length },
    });
  }

  // now we get all blocks in one go
  const blockNumbers = lodash.uniq(events.map((event) => event.blockNumber));
  const blocks = await Promise.all(
    blockNumbers.map((blockNumber) => {
      return provider.getBlock(blockNumber);
    })
  );

  logger.debug({ msg: "Fetched blocks", data: { total: blocks.length, chain, contractAddress } });
  const blockByNumber = lodash.keyBy(blocks, "number");

  return events.map((rawEvent) => {
    if (!rawEvent.args) {
      throw new Error(`No event args in event ${rawEvent}`);
    }
    return {
      transactionHash: rawEvent.transactionHash,
      blockNumber: rawEvent.blockNumber,
      datetime: new Date(blockByNumber[rawEvent.blockNumber].timestamp * 1000),
      from: rawEvent.args.from,
      to: rawEvent.args.to,
      value: rawEvent.args.value,
    };
  });
}
