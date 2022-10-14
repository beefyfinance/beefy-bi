import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import { flatten, groupBy, zipWith } from "lodash";
import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { batchRpcCalls$, BatchStreamConfig } from "../utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "beefy", component: "vault-transfers" });

export interface ERC20Transfer {
  chain: Chain;

  tokenAddress: string;
  tokenDecimals: number;

  // owner infos
  ownerAddress: string;

  // transaction infos
  blockNumber: number;
  transactionHash: string;

  amountTransfered: Decimal;
  logIndex: number;
}

interface GetTransferCallParams {
  address: string;
  decimals: number;
  fromBlock: number;
  toBlock: number;
  // if provided, we only care about transfers from and to this address
  trackAddress?: string;
}

export function fetchErc20Transfers$<TTarget, TObj extends ImportQuery<TTarget, number>, TRes extends ImportQuery<TTarget, number>>(options: {
  rpcConfig: RpcConfig;
  chain: Chain;
  getQueryParams: (obj: TObj) => Omit<GetTransferCallParams, "fromBlock" | "toBlock">;
  emitErrors: ErrorEmitter<TTarget, number>;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchRpcCalls$({
    streamConfig: options.streamConfig,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 2,
    },
    logInfos: { msg: "Fetching ERC20 transfers", data: { chain: options.chain } },
    emitErrors: options.emitErrors,
    formatOutput: options.formatOutput,
    getQuery: (obj): GetTransferCallParams => {
      const params = options.getQueryParams(obj);
      return {
        ...params,
        fromBlock: obj.range.from,
        toBlock: obj.range.to,
      };
    },
    processBatch: (provider, contractCalls: GetTransferCallParams[]) => fetchERC20TransferEvents(provider, options.chain, contractCalls),
    rpcConfig: options.rpcConfig,
  });
}

// when hitting a staking contract we don't have a token in return
// so the balance of the amount we send is our positive diff
export function fetchERC20TransferToAStakingContract$<
  TTarget,
  TObj extends ImportQuery<TTarget, number>,
  TRes extends ImportQuery<TTarget, number>,
>(options: {
  rpcConfig: RpcConfig;
  chain: Chain;
  getQueryParams: (obj: TObj) => Omit<GetTransferCallParams, "fromBlock" | "toBlock">;
  emitErrors: ErrorEmitter<TTarget, number>;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return fetchErc20Transfers$<TTarget, TObj, TRes>({
    ...options,
    formatOutput: (item, transfers) => {
      const params = options.getQueryParams(item);
      const contractAddress = params.trackAddress;
      if (!contractAddress) {
        throw new ProgrammerError({ msg: "Missing trackAddress", params });
      }
      return options.formatOutput(
        item,
        transfers.map(
          (transfer): ERC20Transfer => ({
            ...transfer,
            // fake a token at the staking contract address
            tokenAddress: contractAddress,
            // amounts are reversed because we are sending token to the vault, but we then have a positive balance
            amountTransfered: transfer.amountTransfered.negated(),
          }),
        ),
      );
    },
  });
}

/**
 * Make a batched call to the RPC for all the given contract calls
 * Returns the results in the same order as the contract calls
 */
async function fetchERC20TransferEvents(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: GetTransferCallParams[],
): Promise<ERC20Transfer[][]> {
  if (contractCalls.length === 0) {
    return [];
  }

  logger.debug({
    msg: "Fetching transfer events",
    data: { chain, contractCalls: contractCalls.length },
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

    let fromPromise: Promise<ethers.Event[]>;
    let toPromise: Promise<ethers.Event[]>;

    if (contractCall.trackAddress) {
      const fromFilter = contract.filters.Transfer(contractCall.trackAddress, null);
      const toFilter = contract.filters.Transfer(null, contractCall.trackAddress);
      fromPromise = contract.queryFilter(fromFilter, contractCall.fromBlock, contractCall.toBlock);
      toPromise = contract.queryFilter(toFilter, contractCall.fromBlock, contractCall.toBlock);
    } else {
      const eventFilter = contract.filters.Transfer();
      fromPromise = contract.queryFilter(eventFilter, contractCall.fromBlock, contractCall.toBlock);
      toPromise = Promise.resolve([]);
    }

    // apply decimals and format the events
    eventsPromises.push(
      Promise.all([fromPromise, toPromise])
        .then(([from, to]) => from.concat(to))
        .then((events) =>
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
        ),
    );
  }
  const eventsRes = await Promise.all(eventsPromises);

  const eventCount = eventsRes.reduce((acc, events) => acc + events.length, 0);
  if (eventCount > 0) {
    logger.trace({
      msg: "Got transfer events",
      data: { chain, contractCalls: contractCalls.length, eventCount },
    });
  }

  return zipWith(contractCalls, eventsRes, (contractCall, events) => {
    // we have "from-to" transfers, we need to split them into "from" and "to" transfers
    const allTransfers = flatten(
      events.map((event): ERC20Transfer[] => [
        {
          chain: chain,
          tokenAddress: contractCall.address,
          tokenDecimals: contractCall.decimals,
          ownerAddress: event.from,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransfered: event.value.negated(),
          logIndex: event.logIndex,
        },
        {
          chain: chain,
          tokenAddress: contractCall.address,
          tokenDecimals: contractCall.decimals,
          ownerAddress: event.to,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransfered: event.value,
          logIndex: event.logIndex,
        },
      ]),
    );

    // there could be incoming and outgoing transfers in the same block for the same user
    // we want to merge those into a single transfer
    const transfersByOwnerAndBlock = Object.values(
      groupBy(allTransfers, (transfer) => `${transfer.tokenAddress}-${transfer.ownerAddress}-${transfer.blockNumber}`),
    );
    const transfers = transfersByOwnerAndBlock.map((transfers) => {
      // get the total amount
      let totalDiff = new Decimal(0);
      for (const transfer of transfers) {
        totalDiff = totalDiff.add(transfer.amountTransfered);
      }
      // for the trx hash, we use the last transaction (order by logIndex)
      const lastTrxHash = transfers.sort((a, b) => b.logIndex - a.logIndex)[0].transactionHash;

      return { ...transfers[0], transactionHash: lastTrxHash, sharesBalanceDiff: totalDiff };
    });

    // sanity check
    if (process.env.NODE_ENV === "development") {
      for (const transfer of transfers) {
        if (transfer.blockNumber < contractCall.fromBlock || transfer.blockNumber > contractCall.toBlock) {
          throw new ProgrammerError({
            msg: "Invalid block number",
            data: { transfer, contractCall },
          });
        }
      }
    }

    return transfers;
  });
}
