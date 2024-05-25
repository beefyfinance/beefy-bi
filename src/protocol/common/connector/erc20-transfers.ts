import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import { flatten, groupBy, zipWith } from "lodash";
import { Chain } from "../../../types/chain";
import { ERC20AbiInterface } from "../../../utils/abi";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { RPCBatchCallResult, batchRpcCalls$ } from "../utils/batch-rpc-calls";

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

  amountTransferred: Decimal;
  logIndex: number;
  logLineage: "etherscan" | "rpc";
}

interface GetTransferCallParams {
  tokenAddress: string;
  decimals: number;
  fromBlock: number;
  toBlock: number;
  // if provided, we only care about transfers from and to this address
  trackAddress?: string;
}

export function fetchErc20Transfers$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getQueryParams: (obj: TObj) => GetTransferCallParams;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}) {
  const logData = {
    chain: options.ctx.chain,
    maxGetLogsAddressBatchSize: options.ctx.rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize,
    streamConfig: options.ctx.streamConfig,
  };
  return batchRpcCalls$({
    ctx: options.ctx,
    emitError: options.emitError,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 2,
      eth_getTransactionReceipt: 0,
    },
    logInfos: {
      msg: "Fetching ERC20 transfers without address batch",
      data: logData,
    },
    getQuery: options.getQueryParams,
    processBatch: (provider, contractCalls: GetTransferCallParams[]) => fetchERC20TransferEventsFromRpc(provider, options.ctx.chain, contractCalls),
    formatOutput: options.formatOutput,
  });
}

// when hitting a staking contract we don't have a token in return
// so the balance of the amount we send is our positive diff
export function fetchERC20TransferToAStakingContract$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getQueryParams: (obj: TObj) => GetTransferCallParams;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}) {
  return fetchErc20Transfers$({
    ctx: options.ctx,
    emitError: options.emitError,
    getQueryParams: options.getQueryParams,
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
            amountTransferred: transfer.amountTransferred.negated(),
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
async function fetchERC20TransferEventsFromRpc(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: GetTransferCallParams[],
): Promise<RPCBatchCallResult<GetTransferCallParams, ERC20Transfer[]>> {
  if (contractCalls.length === 0) {
    return { successes: new Map(), errors: new Map() };
  }

  logger.debug({
    msg: "Fetching transfer events from RPC",
    data: { chain, contractCalls: contractCalls.length },
  });

  const eventsPromises: Promise<ethers.Event[]>[] = [];
  for (const contractCall of contractCalls) {
    const contract = new ethers.Contract(contractCall.tokenAddress, ERC20AbiInterface, provider);

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
    eventsPromises.push(Promise.all([fromPromise, toPromise]).then(([from, to]) => from.concat(to)));
  }
  const eventsRes = await Promise.all(eventsPromises);

  const eventCount = eventsRes.reduce((acc, events) => acc + events.length, 0);
  if (eventCount > 0) {
    logger.trace({
      msg: "Got transfer events from RPC",
      data: { chain, contractCalls: contractCalls.length, eventCount },
    });
  }

  return {
    successes: new Map(
      zipWith(contractCalls, eventsRes, (contractCall, events) => {
        const transfers = eventsToTransfers(chain, contractCall, events, "rpc");
        return [contractCall, transfers];
      }),
    ),
    errors: new Map(),
  };
}

/**
 * Transforms the RPC log events to transfers
 * When a transfer is made from A to B, the RPC will return 1 log event
 * but we want to split it into 2 transfers: a negative transfer from A and a positive transfer to B
 * We may also have multiple log transfers inside the same block for the same user
 * We want to merge those into a single transfer by summing the amounts
 */
function eventsToTransfers(
  chain: Chain,
  contractCall: GetTransferCallParams,
  events: ethers.Event[],
  logLineage: "etherscan" | "rpc",
): ERC20Transfer[] {
  // intermediate format
  interface TransferEvent {
    transactionHash: string;
    from: string;
    to: string;
    value: Decimal;
    blockNumber: number;
    logIndex: number;
  }
  const valueMultiplier = new Decimal(10).pow(-contractCall.decimals);

  // we have "from-to" transfers, we need to split them into "from" and "to" transfers
  const allTransfers = flatten(
    events
      .map(
        (event): TransferEvent => ({
          transactionHash: event.transactionHash,
          from: event.args?.from,
          to: event.args?.to,
          value: valueMultiplier.mul(event.args?.value.toString() ?? "0"),
          blockNumber: event.blockNumber,
          logIndex: event.logIndex,
        }),
      )
      .map((event): ERC20Transfer[] => [
        {
          chain: chain,
          tokenAddress: contractCall.tokenAddress,
          tokenDecimals: contractCall.decimals,
          ownerAddress: event.from,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransferred: event.value.negated(),
          logIndex: event.logIndex,
          logLineage,
        },
        {
          chain: chain,
          tokenAddress: contractCall.tokenAddress,
          tokenDecimals: contractCall.decimals,
          ownerAddress: event.to,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransferred: event.value,
          logIndex: event.logIndex,
          logLineage,
        },
      ]),
  );

  const transfers = mergeErc20Transfers(allTransfers);

  // sanity check
  if (process.env.NODE_ENV === "development") {
    for (const transfer of transfers) {
      if (transfer.blockNumber < contractCall.fromBlock || transfer.blockNumber > contractCall.toBlock) {
        throw new ProgrammerError({
          msg: "Invalid block number from explorer",
          data: { transfer, contractCall },
        });
      }
    }
  }

  return transfers;
}

export function mergeErc20Transfers(allTransfers: ERC20Transfer[]) {
  // there could be incoming and outgoing transfers in the same block for the same user
  // we want to merge those into a single transfer to only store "net transfers"
  const transfersByOwnerAndBlock = Object.values(
    groupBy(allTransfers, (transfer) => `${transfer.tokenAddress}-${transfer.ownerAddress}-${transfer.blockNumber}`),
  );
  return transfersByOwnerAndBlock
    .map((transfers): ERC20Transfer => {
      // get the total amount
      let totalDiff = new Decimal(0);
      for (const transfer of transfers) {
        totalDiff = totalDiff.add(transfer.amountTransferred);
      }
      // for the trx hash, we use the last transaction (order by logIndex)
      // but only considering the transfers that have a non-zero amount
      const sortedTransfers = transfers.sort((a, b) => b.logIndex - a.logIndex);
      const nonZeroTransfers = sortedTransfers.filter((transfer) => !transfer.amountTransferred.isZero());
      const transactionHash =
        nonZeroTransfers.length > 0 ? nonZeroTransfers[0].transactionHash : sortedTransfers.length > 0 ? sortedTransfers[0].transactionHash : "";

      return { ...transfers[0], transactionHash, amountTransferred: totalDiff };
    })
    .filter((transfer) => !transfer.amountTransferred.isZero());
}
