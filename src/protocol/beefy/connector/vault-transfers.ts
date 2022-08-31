import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import { Chain } from "../../../types/chain";
import { flatten, zipWith } from "lodash";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

export async function fetchBeefyVaultV6Transfers(
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
    const eventFilter = contract.filters.Transfer();
    const eventsPromise = contract.queryFilter(eventFilter, fromBlock, toBlock);
    eventsPromises.push(eventsPromise);
  }
  const eventsRes = await Promise.all(eventsPromises);

  const events = flatten(
    zipWith(contractAddresses, eventsRes, (contractAddress, events) =>
      events.map((event) => ({
        contractAddress,
        from: event.args?.from,
        to: event.args?.to,
        value: event.args?.value,
        args: event.args,
        blockNumber: event.blockNumber,
        transactionHash: event.transactionHash,
      }))
    )
  );

  logger.debug({
    msg: "Got events for range",
    data: { chain, contractAddresses, fromBlock, toBlock, total: events.length },
  });

  return events;
}
