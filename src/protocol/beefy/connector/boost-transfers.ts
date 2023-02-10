import Decimal from "decimal.js";
import { ethers } from "ethers";
import { flatten, get, groupBy, uniq } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { BeefyBoostAbiInterface } from "../../../utils/abi";
import { getChainWNativeTokenAddress } from "../../../utils/addressbook";
import { ContractWithMultiAddressGetLogs, JsonRpcProviderWithMultiAddressGetLogs, MultiAddressEventFilter } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ERC20Transfer, fetchERC20TransferToAStakingContract$ } from "../../common/connector/erc20-transfers";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

interface BeefyBoostTransferCallParams {
  boostAddress: string;
  stakedTokenAddress: string;
  stakedTokenDecimals: number;
  fromBlock: number;
  toBlock: number;
}

export function fetchBeefyBoostTransfers$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends BeefyBoostTransferCallParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  batchAddressesIfPossible: boolean;
  getBoostTransfersCallParams: (obj: TObj) => TParams;
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
        const objAndCallParams = objs.map((obj) => ({ obj, contractCall: options.getBoostTransfersCallParams(obj) }));
        const resMap = await callLockProtectedRpc(
          () =>
            fetchBoostTransfersUsingAddressBatching(
              options.ctx.rpcConfig.linearProvider,
              options.ctx.chain,
              objAndCallParams.map(({ contractCall }) => contractCall),
            ),
          {
            chain: options.ctx.chain,
            rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
            logInfos: { msg: "Fetching Beefy Boost transfers", data: { chain: options.ctx.chain } },
            maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
            noLockIfNoLimit: true,
            provider: options.ctx.rpcConfig.linearProvider,
          },
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
    return Rx.pipe(
      // fetch latest transfers from and to the boost contract
      fetchERC20TransferToAStakingContract$({
        ctx: options.ctx,
        emitError: options.emitError,
        getQueryParams: (obj) => {
          // for gov vaults we don't have a share token so we use the underlying token
          // transfers and filter on those transfer from and to the contract address
          const callParams = options.getBoostTransfersCallParams(obj);
          return {
            address: callParams.stakedTokenAddress,
            decimals: callParams.stakedTokenDecimals,
            trackAddress: callParams.boostAddress,
            fromBlock: callParams.fromBlock,
            toBlock: callParams.toBlock,
          };
        },
        formatOutput: options.formatOutput,
      }),
    );
  }
}

async function fetchBoostTransfersUsingAddressBatching(
  provider: JsonRpcProviderWithMultiAddressGetLogs,
  chain: Chain,
  contractCalls: BeefyBoostTransferCallParams[],
): Promise<Map<BeefyBoostTransferCallParams, ERC20Transfer[]>> {
  if (contractCalls.length === 0) {
    return new Map();
  }

  // can only do batching if all from/to blocks are the same
  const allFrom = uniq(contractCalls.map((call) => call.fromBlock));
  const allTo = uniq(contractCalls.map((call) => call.toBlock));
  if (allFrom.length > 1 || allTo.length > 1) {
    throw new ProgrammerError({ msg: "Can't batch addresses with different query ranges", data: { allFrom, allTo } });
  }

  // instanciate any ERC20 contract to get the event filter topics
  const contract = new ContractWithMultiAddressGetLogs(getChainWNativeTokenAddress(chain), BeefyBoostAbiInterface, provider);
  const singleStakedEventFilter = contract.filters.Staked();
  const singleWithdrawnEventFilter = contract.filters.Withdrawn();

  const stakedEvents = await contract.queryFilterMultiAddress(
    {
      address: contractCalls.map((call) => call.boostAddress),
      topics: singleStakedEventFilter.topics,
    },
    allFrom[0],
    allTo[0],
  );

  const withdrawnEvents = await contract.queryFilterMultiAddress(
    {
      address: contractCalls.map((call) => call.boostAddress),
      topics: singleWithdrawnEventFilter.topics,
    },
    allFrom[0],
    allTo[0],
  );

  const stakedEventsByAddress = groupBy(stakedEvents, (event) => event.address.toLocaleLowerCase());
  const withdrawnEventsByAddress = groupBy(withdrawnEvents, (event) => event.address.toLocaleLowerCase());
  return new Map(
    contractCalls.map((contractCall) => {
      const stakedEvents = stakedEventsByAddress[contractCall.boostAddress.toLocaleLowerCase()] || [];
      const withdrawnEvents = withdrawnEventsByAddress[contractCall.boostAddress.toLocaleLowerCase()] || [];
      const stakedTransfers = eventsToTransfers(chain, contractCall, stakedEvents, "rpc", "staked");
      const withdrawnTransfers = eventsToTransfers(chain, contractCall, withdrawnEvents, "rpc", "withdrawn");
      return [contractCall, [...stakedTransfers, ...withdrawnTransfers]];
    }),
  );
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
  contractCall: BeefyBoostTransferCallParams,
  events: ethers.Event[],
  logLineage: "etherscan" | "rpc",
  type: "staked" | "withdrawn",
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
  const valueMultiplier = new Decimal(10).pow(-contractCall.stakedTokenDecimals);

  // we have "from-to" transfers, we need to split them into "from" and "to" transfers
  const allTransfers = flatten(
    events
      .map(
        (event): TransferEvent => ({
          transactionHash: event.transactionHash,
          from: type === "staked" ? event.args?.user : contractCall.boostAddress,
          to: type === "staked" ? contractCall.boostAddress : event.args?.user,
          // if it's a stake event, the amount is negative (we're removing tokens from the user)
          // but we want a positive amount for the transfer since we're adding tokens to the boost contract
          // so it's considered a positive investment
          value: valueMultiplier.mul(event.args?.amount.toString() ?? "0").mul(type === "staked" ? -1 : 1),
          blockNumber: event.blockNumber,
          logIndex: event.logIndex,
        }),
      )
      .map((event): ERC20Transfer[] => [
        {
          chain: chain,
          tokenAddress: contractCall.stakedTokenAddress,
          tokenDecimals: contractCall.stakedTokenDecimals,
          ownerAddress: event.from,
          blockNumber: event.blockNumber,
          transactionHash: event.transactionHash,
          amountTransferred: event.value.negated(),
          logIndex: event.logIndex,
          logLineage,
        },
        {
          chain: chain,
          tokenAddress: contractCall.stakedTokenAddress,
          tokenDecimals: contractCall.stakedTokenDecimals,
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
