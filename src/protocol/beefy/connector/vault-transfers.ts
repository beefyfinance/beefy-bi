import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import { Chain } from "../../../types/chain";
import { flatten, zipWith } from "lodash";
import { TokenizedVaultUserAction } from "../../types/connector";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

export async function fetchBeefyVaultV6Transfers(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractAddresses: string[],
  fromBlock?: number,
  toBlock?: number,
): Promise<TokenizedVaultUserAction[]> {
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
      flatten(
        events.map((event): [TokenizedVaultUserAction, TokenizedVaultUserAction] => [
          {
            chain: chain,
            vaultAddress: contractAddress,
            ownerAddress: event.args?.from,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            sharesBalanceDiff: event.args?.value.mul(-1),
          },
          {
            chain: chain,
            vaultAddress: contractAddress,
            ownerAddress: event.args?.to,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            sharesBalanceDiff: event.args?.value,
          },
        ]),
      ),
    ),
  );

  logger.debug({
    msg: "Got events for range",
    data: { chain, contractAddresses, fromBlock, toBlock, total: events.length },
  });

  return events;
}
