import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { flatten, zipWith } from "lodash";
import { TokenizedVaultUserTransfer } from "../../types/connector";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

export async function fetchBeefyVaultV6Transfers(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  erc20Contracts: { address: string; decimals: number }[],
  fromBlock?: number,
  toBlock?: number,
): Promise<TokenizedVaultUserTransfer[]> {
  logger.debug({
    msg: "Fetching withdraw and deposits for vault",
    data: { chain, count: erc20Contracts.length, fromBlock, toBlock },
  });

  // fetch all contract logs in one call
  interface TransferEvent {
    transactionHash: string;
    from: string;
    to: string;
    value: Decimal;
    blockNumber: number;
  }
  const eventsPromises: Promise<TransferEvent[]>[] = [];
  for (const erc20Contract of erc20Contracts) {
    const valueMultiplier = new Decimal(10).pow(-erc20Contract.decimals);
    const contract = new ethers.Contract(erc20Contract.address, ERC20Abi, provider);
    const eventFilter = contract.filters.Transfer();
    const eventsPromise = contract.queryFilter(eventFilter, fromBlock, toBlock).then((events) =>
      events.map((event) => ({
        transactionHash: event.transactionHash,
        from: event.args?.from,
        to: event.args?.to,
        value: valueMultiplier.mul(event.args?.value.toString() ?? "0"),
        blockNumber: event.blockNumber,
      })),
    );
    eventsPromises.push(eventsPromise);
  }
  const eventsRes = await Promise.all(eventsPromises);

  const events = flatten(
    zipWith(erc20Contracts, eventsRes, (contract, events) =>
      flatten(
        events.map((event): [TokenizedVaultUserTransfer, TokenizedVaultUserTransfer] => [
          {
            chain: chain,
            vaultAddress: contract.address,
            sharesDecimals: contract.decimals,
            ownerAddress: event.from,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            sharesBalanceDiff: event.value.negated(),
          },
          {
            chain: chain,
            vaultAddress: contract.address,
            sharesDecimals: contract.decimals,
            ownerAddress: event.from,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            sharesBalanceDiff: event.value,
          },
        ]),
      ),
    ),
  );

  logger.debug({
    msg: "Got events for range",
    data: { chain, count: erc20Contracts.length, fromBlock, toBlock, total: events.length },
  });

  return events;
}
