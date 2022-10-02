import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { rootLogger } from "../../../utils/logger";
import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { groupBy, zipWith } from "lodash";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { DbProduct } from "../loader/product";
import { getRpcRetryConfig } from "../utils/rpc-retry-config";
import { backOff } from "exponential-backoff";

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

export function fetchErc20Transfers$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  provider: ethers.providers.JsonRpcProvider;
  chain: Chain;
  getQueryParams: (obj: TObj) => Omit<GetTransferCallParams, "fromBlock" | "toBlock">;
  emitErrors: ErrorEmitter;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const logInfos = { msg: "Fetching ERC20 transfers", data: { chain: options.chain } };
  const retryConfig = getRpcRetryConfig({ logInfos, maxTotalRetryMs: options.streamConfig.maxTotalRetryMs });

  return Rx.pipe(
    // take a batch of items
    Rx.bufferTime(options.streamConfig.maxInputWaitMs, undefined, options.streamConfig.maxInputTake),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs: TObj[]) => {
      const contractCalls = objs.map((obj) => {
        const params = options.getQueryParams(obj);
        return {
          ...params,
          fromBlock: obj.blockRange.from,
          toBlock: obj.blockRange.to,
        };
      });

      try {
        const transfers = await backOff(() => fetchERC20TransferEvents(options.provider, options.chain, contractCalls), retryConfig);
        return zipWith(objs, transfers, options.formatOutput);
      } catch (err) {
        // here, none of the retrying worked, so we emit all the objects as in error
        logger.error({ msg: "Error fetching ERC20 transfers", err });
        logger.error(err);
        for (const obj of objs) {
          options.emitErrors(obj);
        }
        return Rx.EMPTY;
      }
    }),

    // flatten
    Rx.mergeAll(),
  );
}

// when hitting a staking contract we don't have a token in return
// so the balance of the amount we send is our positive diff
export function fetchERC20TransferToAStakingContract$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  provider: ethers.providers.JsonRpcProvider;
  chain: Chain;
  getQueryParams: (obj: TObj) => Omit<GetTransferCallParams, "fromBlock" | "toBlock">;
  emitErrors: ErrorEmitter;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return fetchErc20Transfers$<TProduct, TObj, TRes>({
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

    // we make 2 calls per "contract call", one for each side of the transfer
    // this is because we can't filter by "from" and "to" at the same time
    // consequently, callPromises will always be twice as long as contractCalls
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

  return contractCalls.map((contractCall, i) => {
    const fromEvents = eventsRes[i];
    const toEvents = eventsRes[i + 1];
    const fromTransfers = fromEvents.map(
      (event): ERC20Transfer => ({
        chain: chain,
        tokenAddress: contractCall.address,
        tokenDecimals: contractCall.decimals,
        ownerAddress: event.from,
        blockNumber: event.blockNumber,
        transactionHash: event.transactionHash,
        amountTransfered: event.value.negated(),
        logIndex: event.logIndex,
      }),
    );
    const toTransfers = toEvents.map(
      (event): ERC20Transfer => ({
        chain: chain,
        tokenAddress: contractCall.address,
        tokenDecimals: contractCall.decimals,
        ownerAddress: event.to,
        blockNumber: event.blockNumber,
        transactionHash: event.transactionHash,
        amountTransfered: event.value,
        logIndex: event.logIndex,
      }),
    );
    const allTransfers: ERC20Transfer[] = [...fromTransfers, ...toTransfers];

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

    if (transfers.length > 0) {
      logger.debug({
        msg: "Got transfers for range",
        data: { chain, contractCall, transferCount: transfers.length },
      });
    }
    return transfers;
  });
}
