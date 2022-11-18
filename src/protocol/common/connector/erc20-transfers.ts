import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import { flatten, groupBy, uniqBy, zipWith } from "lodash";
import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range, rangeMerge } from "../../../utils/range";
import { RpcLimitations } from "../../../utils/rpc/rpc-limitations";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

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
}

interface GetTransferCallParams {
  address: string;
  decimals: number;
  fromBlock: number;
  toBlock: number;
  // if provided, we only care about transfers from and to this address
  trackAddress?: string;
}

export function fetchErc20Transfers$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  allowFetchingFromEtherscan: boolean;
  getQueryParams: (obj: TObj) => GetTransferCallParams;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}) {
  const fetchERC20FromRPCPipeline$ = batchRpcCalls$({
    ctx: options.ctx,
    emitError: options.emitError,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 2,
      eth_getTransactionReceipt: 0,
    },
    logInfos: { msg: "Fetching ERC20 transfers", data: { chain: options.ctx.chain } },
    getQuery: options.getQueryParams,
    processBatch: (provider, contractCalls: GetTransferCallParams[]) => fetchERC20TransferEventsFromRpc(provider, options.ctx.chain, contractCalls),
    formatOutput: options.formatOutput,
  });

  const isRangeLargeEnoughToUseEtherscan = (range: Range<number>) =>
    range.to - range.from > options.ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan * 10;

  // if possible, fetch a bunch of data from etherscan so we can avoid doing a lot of rpc calls
  // since etherscan can return a list of transfers without having to restrict by a block range
  // we can quickly find out if the vault has any transfers at all for the given range
  if (options.ctx.rpcConfig.etherscan && options.allowFetchingFromEtherscan) {
    logger.debug({ msg: "Fetching transfers from etherscan provider", data: { chain: options.ctx.chain } });
    const etherscanConfig = options.ctx.rpcConfig.etherscan;
    return Rx.pipe(
      Rx.pipe(
        // call getQueryParams on each object
        Rx.map((obj: TObj) => ({ obj, params: options.getQueryParams(obj) })),
        // make sure we are using this properly
        Rx.tap((item) => {
          if (item.params.trackAddress) {
            throw new ProgrammerError({ msg: "etherscan fetching does not support trackAddress", data: { params: item.params } });
          }
        }),
      ),
      Rx.pipe(
        Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, options.ctx.streamConfig.maxInputTake),
        Rx.filter((items) => items.length > 0),
      ),

      // group by address since we can only fetch one address at a time
      Rx.concatMap((items) => {
        logger.trace({ msg: "Fetching transfers from etherscan provider", data: { chain: options.ctx.chain, items: items.length } });
        // merge ranges by address for all the items so we can fetch a bunch of data at once
        const itemsByAddress = groupBy(items, (item) => item.params.address);
        return Object.entries(itemsByAddress).map(([address, items]) => ({ address, items }));
      }),

      // merge the input range for all items for the same address
      Rx.concatMap(({ address, items }) => {
        const itemsRanges = items.map((item) => ({ from: item.params.fromBlock, to: item.params.toBlock }));
        const mergedRanges = rangeMerge(itemsRanges);
        return mergedRanges.map((mergedRange) => ({ address, items, mergedRange }));
      }),

      // for each merge range, if the range is large enough, use the explorer to fetch the data
      // otherwise, use the rpc. We don't want to overload the explorer with too many requests
      // since we may be running multiple rpc pipelines at the same time and we only have one explorer
      Rx.connect((items$) =>
        Rx.merge(
          // go through rpc if the range is not large enough
          // this might happens when we retry sparse ranges
          items$.pipe(
            Rx.filter((item) => !isRangeLargeEnoughToUseEtherscan(item.mergedRange)),
            Rx.tap((item) =>
              logger.trace({
                msg: "Batch too small, fetching transfers from rpc",
                data: { chain: options.ctx.chain, address: item.address, range: item.mergedRange },
              }),
            ),
            Rx.concatMap((item) => item.items.map((item) => item.obj)),
            fetchERC20FromRPCPipeline$,
          ),
          // if we have a large enough range, use the explorer
          items$.pipe(
            Rx.filter((item) => isRangeLargeEnoughToUseEtherscan(item.mergedRange)),
            Rx.tap((item) =>
              logger.trace({
                msg: "Batch fetching transfers from rpc",
                data: { chain: options.ctx.chain, address: item.address, range: item.mergedRange },
              }),
            ),
            Rx.concatMap(async ({ address, items, mergedRange }) => {
              try {
                const resultMap: Map<TObj, ERC20Transfer[]> = new Map();
                const decimals = items[0].params.decimals;
                const params: GetTransferCallParams = { address, decimals, fromBlock: mergedRange.from, toBlock: mergedRange.to };
                const transfers = await fetchERC20TransferEventsFromExplorer(
                  etherscanConfig.provider,
                  etherscanConfig.limitations,
                  options.ctx.chain,
                  params,
                );

                if (transfers.length > 0) {
                  logger.debug({
                    msg: "Fetched transfers from etherscan provider",
                    data: { chain: options.ctx.chain, address, items: items.length, transfers: transfers.length },
                  });
                } else {
                  logger.trace({
                    msg: "No transfers found from etherscan provider",
                    data: { chain: options.ctx.chain, address, items: items.length, transfers: transfers.length },
                  });
                }

                // reassign the transfers to the input items
                for (const item of items) {
                  if (resultMap.has(item.obj)) {
                    throw new ProgrammerError({ msg: "duplicate item", data: { item } });
                  }
                  const itemTransfers = transfers.filter(
                    (transfer) => transfer.blockNumber >= item.params.fromBlock && transfer.blockNumber <= item.params.toBlock,
                  );
                  resultMap.set(item.obj, itemTransfers);
                }
                return Array.from(resultMap.entries()).map(([obj, transfers]) => options.formatOutput(obj, transfers));
              } catch (err) {
                logger.error({ msg: "Error fetching transfers from etherscan provider", data: { chain: options.ctx.chain, address, err } });
                for (const item of items) {
                  options.emitError(item.obj);
                }
                return Rx.EMPTY;
              }
            }),
            // format the output as expected
            Rx.concatAll(),
          ),
        ),
      ),
    );
  }

  // otherwise, just fetch from the rpc
  return fetchERC20FromRPCPipeline$;
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
    allowFetchingFromEtherscan: false, // we don't support this for now
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
): Promise<Map<GetTransferCallParams, ERC20Transfer[]>> {
  if (contractCalls.length === 0) {
    return new Map();
  }

  logger.debug({
    msg: "Fetching transfer events from RPC",
    data: { chain, contractCalls: contractCalls.length },
  });

  const eventsPromises: Promise<ethers.Event[]>[] = [];
  for (const contractCall of contractCalls) {
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

  return new Map(
    zipWith(contractCalls, eventsRes, (contractCall, events) => {
      const transfers = eventsToTransfers(chain, contractCall, events);
      return [contractCall, transfers];
    }),
  );
}

/**
 * Make a batched call to the RPC for all the given contract calls
 * Returns the results in the same order as the contract calls
 */
export async function fetchERC20TransferEventsFromExplorer(
  provider: ethers.providers.EtherscanProvider,
  limitations: RpcLimitations,
  chain: Chain,
  contractCall: GetTransferCallParams,
): Promise<ERC20Transfer[]> {
  logger.debug({
    msg: "Fetching transfer events from explorer",
    data: { chain, contractCall },
  });

  const contract = new ethers.Contract(contractCall.address, ERC20Abi, provider);

  if (contractCall.trackAddress) {
    throw new ProgrammerError({ msg: "Tracking not implemented for etherscan", contractCall });
  }

  const eventFilter = contract.filters.Transfer();

  // etherscan returns logs in time order ascending and limits results to 1000
  // we want to continue fetching as long as we have 1000 results in the list
  let fromBlock = contractCall.fromBlock;
  let toBlock = contractCall.toBlock;
  let allEvents: ethers.Event[] = [];
  let maxLoops = 100;
  while (maxLoops-- > 0) {
    const events = await callLockProtectedRpc(() => contract.queryFilter(eventFilter, fromBlock, toBlock), {
      chain: chain,
      logInfos: { msg: "Fetching ERC20 transfers from etherscan", data: { chain, contractCall } },
      maxTotalRetryMs: 1000 * 60 * 5,
      provider: provider,
      rpcLimitations: limitations,
      noLockIfNoLimit: true, // etherscan is rate limited so this has no effect
    });

    // here, we have all the events we can get from etherscan
    if (events.length < 1000) {
      allEvents = allEvents.concat(events);
      break;
    }

    // we have 1000 events, we need to fetch more
    // remove the events from the last block we fetched
    const lastBlock = events[events.length - 1].blockNumber;
    const eventsToAdd = events.filter((event) => event.blockNumber < lastBlock);
    allEvents = allEvents.concat(eventsToAdd);
    fromBlock = lastBlock;
  }
  const eventCount = allEvents.length;
  if (eventCount > 0) {
    logger.trace({
      msg: "Got transfer events from explorer",
      data: { chain, eventCount, contractCall },
    });
  }
  const transfers = eventsToTransfers(chain, contractCall, allEvents);
  return transfers;
}

/**
 * Transforms the RPC log events to transfers
 * When a transfer is made from A to B, the RPC will return 1 log event
 * but we want to split it into 2 transfers: a negative transfer from A and a positive transfer to B
 * We may also have multiple log transfers inside the same block for the same user
 * We want to merge those into a single transfer by summing the amounts
 */
function eventsToTransfers(chain: Chain, contractCall: GetTransferCallParams, events: ethers.Event[]): ERC20Transfer[] {
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
    uniqBy(events, (event) => `${event.transactionHash}-${event.logIndex}`)
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
          tokenAddress: contractCall.address,
          tokenDecimals: contractCall.decimals,
          ownerAddress: event.from,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransferred: event.value.negated(),
          logIndex: event.logIndex,
        },
        {
          chain: chain,
          tokenAddress: contractCall.address,
          tokenDecimals: contractCall.decimals,
          ownerAddress: event.to,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransferred: event.value,
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
      totalDiff = totalDiff.add(transfer.amountTransferred);
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
          msg: "Invalid block number from explorer",
          data: { transfer, contractCall },
        });
      }
    }
  }
  return transfers;
}
