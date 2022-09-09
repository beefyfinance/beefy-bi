import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { flatten, groupBy, min, uniq, zipWith } from "lodash";
import { TokenizedVaultUserTransfer } from "../../types/connector";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

interface GetTransferCallParams {
  address: string;
  decimals: number;
  fromBlock: number;
  toBlock: number;
  // if provided, we only care about transfers from and to this address
  trackAddress?: string;
}

export function mapErc20Transfers<TObj, TKey extends string, TParams extends GetTransferCallParams>(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: TokenizedVaultUserTransfer[] })[]> {
  // we want to make a query for all requested block numbers of this contract
  const toQueryObj = (objs: TObj[]): TParams => {
    const params = objs.map(getParams);
    const trackAddresses = uniq(params.map((p) => p.trackAddress).filter((a) => a));
    if (trackAddresses.length > 1) {
      throw new ProgrammerError({ msg: "Tracking more than one address per batch is not yet supported" });
    }
    const trackAddress = trackAddresses.length === 0 ? undefined : trackAddresses[0];
    return {
      ...params[0],
      fromBlock: min(params.map((p) => p.fromBlock)),
      toBlock: min(params.map((p) => p.toBlock)),
      trackAddress,
    };
  };
  const getKeyFromObj = (obj: TObj) => getKeyFromParams(getParams(obj));
  const getKeyFromParams = ({ address }: TParams) => {
    return address.toLocaleLowerCase();
  };

  const process = async (params: TParams[]) => {
    const transfers = await fetchERC20TransferEvents(provider, chain, params);
    // make sure we return data in the same order as the input
    const grouped = groupBy(transfers, (t) => t.vaultAddress);
    return params.map((p) => grouped[p.address] || [] /* defaults to no transfers */);
  };

  return batchQueryGroup(toQueryObj, getKeyFromObj, process, toKey);
}

async function fetchERC20TransferEvents(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: GetTransferCallParams[],
): Promise<TokenizedVaultUserTransfer[]> {
  logger.debug({
    msg: "Fetching withdraw and deposits for vault",
    data: { chain, count: contractCalls.length },
  });

  // fetch all contract logs in one call
  interface TransferEvent {
    transactionHash: string;
    from: string;
    to: string;
    value: Decimal;
    blockNumber: number;
    logIndex: number;
  }
  const eventsPromises: Promise<TransferEvent[]>[] = [];
  for (const contractCall of contractCalls) {
    const valueMultiplier = new Decimal(10).pow(-contractCall.decimals);
    const contract = new ethers.Contract(contractCall.address, ERC20Abi, provider);
    const callPromises: Promise<ethers.Event[]>[] = [];

    if (contractCall.trackAddress) {
      const fromFilter = contract.filters.Transfer(contractCall.trackAddress, null);
      const toFilter = contract.filters.Transfer(null, contractCall.trackAddress);
      const fromPromise = contract.queryFilter(fromFilter, contractCall.fromBlock, contractCall.toBlock);
      const toPromise = contract.queryFilter(toFilter, contractCall.fromBlock, contractCall.toBlock);
      callPromises.push(fromPromise);
      callPromises.push(toPromise);
    } else {
      const eventFilter = contract.filters.Transfer();
      const callPromise = contract.queryFilter(eventFilter, contractCall.fromBlock, contractCall.toBlock);
      callPromises.push(callPromise);
      // make sure we have 2 promises to simplify matching code below
      callPromises.push(Promise.resolve([]));
    }
    // map raw events to TransferEvents
    for (let callPromise of callPromises) {
      const mappedCallPromise = callPromise.then((events) =>
        events.map(
          (event): TransferEvent => ({
            transactionHash: event.transactionHash,
            from: event.args?.from,
            to: event.args?.to,
            value: valueMultiplier.mul(event.args?.value.toString() ?? "0"),
            blockNumber: event.blockNumber,
            logIndex: event.logIndex,
          }),
        ),
      );
      eventsPromises.push(mappedCallPromise);
    }
  }
  const eventsRes = await Promise.all(eventsPromises);

  const doubledContractCall = Array.from({ length: contractCalls.length * 2 }).map(
    (_, i) => contractCalls[Math.floor(i / 2)],
  );
  type TransferWithLogIndex = TokenizedVaultUserTransfer & { logIndex: number };
  const events = flatten(
    zipWith(doubledContractCall, eventsRes, (contract, events) =>
      flatten(
        events.map((event): [TransferWithLogIndex, TransferWithLogIndex] => [
          {
            chain: chain,
            vaultAddress: contract.address,
            sharesDecimals: contract.decimals,
            ownerAddress: event.from,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            sharesBalanceDiff: event.value.negated(),
            logIndex: event.logIndex,
          },
          {
            chain: chain,
            vaultAddress: contract.address,
            sharesDecimals: contract.decimals,
            ownerAddress: event.to,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            sharesBalanceDiff: event.value,
            logIndex: event.logIndex,
          },
        ]),
      ),
    ),
  );

  // there could be incoming and outgoing transfers in the same block for the same user
  // we want to merge those into a single transfer
  const transfersByOwnerAndBlock = Object.values(
    groupBy(events, (event) => `${event.vaultAddress}-${event.ownerAddress}-${event.blockNumber}`),
  );
  const transfers = transfersByOwnerAndBlock.map((transfers) => {
    // get the total amount
    let totalDiff = new Decimal(0);
    for (const transfer of transfers) {
      totalDiff = totalDiff.add(transfer.sharesBalanceDiff);
    }
    // for the trx hash, we use the last transaction (order by logIndex)
    const lastTrxHash = transfers.sort((a, b) => b.logIndex - a.logIndex)[0].transactionHash;

    return { ...transfers[0], transactionHash: lastTrxHash, sharesBalanceDiff: totalDiff };
  });

  logger.debug({
    msg: "Got transfers for range",
    data: { chain, count: contractCalls.length, total: transfers.length },
  });

  return transfers;
}
