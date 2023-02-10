import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import { flatten, groupBy, uniq, zipWith } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { ERC20AbiInterface } from "../../../utils/abi";
import { getChainWNativeTokenAddress } from "../../../utils/addressbook";
import { ContractWithMultiAddressGetLogs, JsonRpcProviderWithMultiAddressGetLogs } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
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
  logLineage: "etherscan" | "rpc";
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
  batchAddressesIfPossible: boolean;
  getQueryParams: (obj: TObj) => GetTransferCallParams;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}) {
  if (options.batchAddressesIfPossible && options.ctx.rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize !== null) {
    const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;

    const maxInputObjsPerBatch = options.ctx.rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize;
    return Rx.pipe(
      // add object TS type
      Rx.tap((_: TObj) => {}),

      // take a batch of items
      Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, maxInputObjsPerBatch),
      Rx.filter((objs) => objs.length > 0),
      Rx.mergeMap(async (objs) => {
        const objAndCallParams = objs.map((obj) => ({ obj, contractCall: options.getQueryParams(obj) }));
        const resMap = await fetchERC20TransferEventsFromRpcUsingAddressBatching(
          options.ctx.rpcConfig.linearProvider,
          options.ctx.chain,
          objAndCallParams.map(({ contractCall }) => contractCall),
        );
        return objAndCallParams.map(({ obj, contractCall }) => {
          const res = resMap.get(contractCall);
          if (!res) {
            throw new ProgrammerError({ msg: "Missing result", data: { contractCall } });
          }
          return options.formatOutput(obj, res);
        });
      }, workConcurrency),
      // flatten
      Rx.mergeAll(),
    );
  } else {
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
      logInfos: { msg: "Fetching ERC20 transfers", data: { chain: options.ctx.chain } },
      getQuery: options.getQueryParams,
      processBatch: (provider, contractCalls: GetTransferCallParams[]) => fetchERC20TransferEventsFromRpc(provider, options.ctx.chain, contractCalls),
      formatOutput: options.formatOutput,
    });
  }
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
    // we can't batch addresses while tracking a specific address since
    // tracking means we need to use topics including the address we track
    // and those topics are unique per tracked address
    batchAddressesIfPossible: false,
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
    const contract = new ethers.Contract(contractCall.address, ERC20AbiInterface, provider);

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
      const transfers = eventsToTransfers(chain, contractCall, events, "rpc");
      return [contractCall, transfers];
    }),
  );
}

/**
 * Make a batched call to the RPC for all the given contract calls
 * This uses the address batching feature of the RPC
 * This is only possible if all the contract calls have the same from/to block
 * and if none of the contract calls have a trackAddress
 *
 * This feature should be natively supported by ethers.js but it's not available yet, should be in v6
 * https://github.com/ethers-io/ethers.js/issues/473#issuecomment-1387042069
 *
 * Returns the results in the same order as the contract calls
 */
async function fetchERC20TransferEventsFromRpcUsingAddressBatching(
  provider: JsonRpcProviderWithMultiAddressGetLogs,
  chain: Chain,
  contractCalls: GetTransferCallParams[],
): Promise<Map<GetTransferCallParams, ERC20Transfer[]>> {
  if (contractCalls.length === 0) {
    return new Map();
  }
  // we can't do batching while tracking a specific address
  const contractCallsWithTrackAddress = contractCalls.filter((call) => call.trackAddress);
  if (contractCallsWithTrackAddress.length > 0) {
    throw new ProgrammerError({ msg: "Can't batch addresses while tracking a specific address", data: { contractCallsWithTrackAddress } });
  }

  // can only do batching if all from/to blocks are the same
  const allFrom = uniq(contractCalls.map((call) => call.fromBlock));
  const allTo = uniq(contractCalls.map((call) => call.toBlock));
  if (allFrom.length > 1 || allTo.length > 1) {
    throw new ProgrammerError({ msg: "Can't batch addresses with different query ranges", data: { allFrom, allTo } });
  }

  logger.debug({
    msg: "Fetching transfer events from RPC with address batching",
    data: { chain, contractCalls: contractCalls.length },
  });

  // instanciate any ERC20 contract to get the event filter topics
  const contract = new ContractWithMultiAddressGetLogs(getChainWNativeTokenAddress(chain), ERC20AbiInterface, provider);
  const eventFilter = contract.filters.Transfer();
  const events = await contract.queryFilter(eventFilter, allFrom[0], allTo[0]);

  if (events.length > 0) {
    logger.trace({
      msg: "Got transfer events from RPC",
      data: { chain, contractCalls: contractCalls.length, eventCount: events.length },
    });
  }

  const eventsByAddress = groupBy(events, (event) => event.address.toLocaleLowerCase());
  return new Map(
    contractCalls.map((contractCall) => {
      const events = eventsByAddress[contractCall.address.toLocaleLowerCase()] || [];
      const transfers = eventsToTransfers(chain, contractCall, events, "rpc");
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

  const contract = new ethers.Contract(contractCall.address, ERC20AbiInterface, provider);

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
  const transfers = eventsToTransfers(chain, contractCall, allEvents, "etherscan");
  return transfers;
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
          tokenAddress: contractCall.address,
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
          tokenAddress: contractCall.address,
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
