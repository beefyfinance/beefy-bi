import Decimal from "decimal.js";
import { ethers } from "ethers";
import { keyBy } from "lodash";
import * as Rx from "rxjs";
import { Hex, decodeEventLog, getEventSelector, parseAbi } from "viem";
import { Chain } from "../../../types/chain";
import { MultiAddressEventFilter, normalizeAddressOrThrow } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range, isInRange } from "../../../utils/range";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ERC20Transfer, mergeErc20Transfers } from "../../common/connector/erc20-transfers";
import { DbBeefyProduct, DbProduct } from "../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx, Throwable } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import {
  AddressBatchOutput,
  JsonRpcBatchOutput,
  QueryOptimizerOutput,
  extractObjsAndRangeFromOptimizerOutput,
  isAddressBatchQueries,
  isJsonRpcBatchQueries,
} from "../../common/utils/optimize-range-queries";
import { getProductContractAddress } from "../utils/contract-accessors";
import { isBeefyBoost, isBeefyGovVault, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "product-events" });

export function fetchProductEvents$<TObj, TQueryContent extends { product: DbBeefyProduct }, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getCallParams: (obj: TObj) => QueryOptimizerOutput<TQueryContent, number>;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const fetchJsonRpcBatch$: Rx.OperatorFunction<
    { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number> },
    { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number>; transfers: ERC20Transfer[] }
  > = Rx.pipe(
    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      logInfos: { msg: "Fetching product events using batch", data: {} },
      rpcCallsPerInputObj: {
        eth_blockNumber: 0,
        eth_call: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 1,
        eth_getTransactionReceipt: 0,
      },
      getQuery: (item) => ({ product: item.query.obj.product, range: item.query.range }),
      processBatch: async (provider, queryObjs) => {
        const events = await Promise.all(
          queryObjs.map(({ product, range }) => {
            const address = getProductContractAddress(product);
            const contract = new ethers.Contract(address, ethersInterface, provider);
            return contract.queryFilter({ address, topics: [eventConfigs.map((e) => e.selector)] }, range.from, range.to);
          }),
        );
        return {
          errors: new Map(),
          successes: new Map(queryObjs.map((obj, i) => [obj, events[i]])),
        };
      },
      formatOutput: (item, rawLogs) => ({ ...item, rawLogs }),
    }),

    Rx.map((item) => {
      const rawLogs = item.rawLogs;

      const logs = rawLogs.map(parseRawLog);

      // decode logs to transfer events
      const product = item.query.obj.product;
      const transfers = mergeErc20Transfers(
        logs
          .filter((log) => !log.removed)
          .filter((log) => filterLogByTypeOfProduct(log, product))
          .flatMap((log): ERC20Transfer[] => logToTransfers(log, { chain: options.ctx.chain, decimals: getProductDecimals(product) })),
      );

      return { ...item, transfers };
    }),
  );

  const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;
  const getLogsAddressBatch$: Rx.OperatorFunction<
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number> },
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number>; transfers: ERC20Transfer[] }
  > = Rx.pipe(
    Rx.tap((item) => logger.trace({ msg: "item", data: item })),

    Rx.mergeMap(async (item) => {
      const provider = options.ctx.rpcConfig.linearProvider;
      const products = item.query.objs.map((o) => o.product);
      const adresses = products.map(getProductContractAddress);
      if (products.length <= 0) {
        logger.error({ msg: "Product list is empty", data: { objs: item.query.objs } });
        throw new ProgrammerError("Empty list of products");
      }

      const filter: MultiAddressEventFilter = {
        address: adresses,
        topics: [eventConfigs.map((e) => e.selector)],
        fromBlock: item.query.range.from,
        toBlock: item.query.range.to,
      };

      try {
        const rawLogs = await callLockProtectedRpc(() => provider.getLogsMultiAddress(filter), {
          chain: options.ctx.chain,
          rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
          logInfos: { msg: "Fetching product events with address batch", data: item },
          maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
          noLockIfNoLimit: true,
          provider: options.ctx.rpcConfig.linearProvider,
        });

        const logs = rawLogs.map(parseRawLog);

        // decode logs to transfer events
        const productByAddress = keyBy(products, (p) => normalizeAddressOrThrow(getProductContractAddress(p)));
        const getProduct = (log: Log) => productByAddress[normalizeAddressOrThrow(log.address)];
        const postFiltersByAddress = keyBy(item.query.postFilters, (f) => normalizeAddressOrThrow(getProductContractAddress(f.obj.product)));
        const transfers = mergeErc20Transfers(
          logs
            .filter((log) => !log.removed)
            .filter((log) => filterLogByTypeOfProduct(log, getProduct(log)))
            .flatMap((log): ERC20Transfer[] => logToTransfers(log, { chain: options.ctx.chain, decimals: getProductDecimals(getProduct(log)) }))
            // apply postfilters
            .filter((transfer) => {
              const postFilter = postFiltersByAddress[normalizeAddressOrThrow(transfer.tokenAddress)];
              if (!postFilter) {
                return true;
              }
              if (postFilter.filter === "no-filter") {
                return true;
              }
              return postFilter.filter.some((r) => isInRange(r, transfer.blockNumber));
            }),
        );

        return [{ ...item, transfers }];
      } catch (e: unknown) {
        const error = e as Throwable;
        // here, none of the retrying worked, so we emit all the objects as in error
        const report: ErrorReport = {
          error,
          infos: { msg: "Error fetching produc tevents", data: { chain: options.ctx.chain, err: error } },
        };
        logger.debug(report.infos);
        logger.debug(report.error);
        options.emitError(item.obj, report);
        return Rx.EMPTY;
      }
    }, workConcurrency),

    Rx.concatAll(),
  );

  return Rx.pipe(
    // wrap obj
    Rx.map((obj: TObj) => ({ obj, query: options.getCallParams(obj) })),

    // handle different types of requests differently
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number> } => isJsonRpcBatchQueries(item.query)),
          fetchJsonRpcBatch$,
        ),
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; query: AddressBatchOutput<TQueryContent, number> } => isAddressBatchQueries(item.query)),
          getLogsAddressBatch$,
        ),
      ),
    ),
    Rx.map(({ obj, transfers }) => options.formatOutput(obj, transfers)),
  );
}

export function extractProductTransfersFromOutputAndTransfers<TObj>(
  output: QueryOptimizerOutput<TObj, number>,
  getProduct: (obj: TObj) => DbBeefyProduct,
  transfers: ERC20Transfer[],
): { obj: TObj; range: Range<number>; transfers: ERC20Transfer[] }[] {
  return extractObjsAndRangeFromOptimizerOutput({ output, objKey: (o) => getProduct(o).productKey }).flatMap(({ obj, range }) => {
    const product = getProduct(obj);
    return {
      obj,
      product,
      range,
      transfers: transfers.filter(
        (t) =>
          normalizeAddressOrThrow(t.tokenAddress) === normalizeAddressOrThrow(getProductContractAddress(product)) && isInRange(range, t.blockNumber),
      ),
    };
  });
}

const getProductDecimals = (product: DbProduct) => {
  if (isBeefyBoost(product)) {
    return product.productData.boost.staked_token_decimals;
  } else if (isBeefyGovVault(product)) {
    return product.productData.vault.want_decimals;
  } else if (isBeefyStandardVault(product)) {
    return product.productData.vault.token_decimals;
  } else {
    throw new ProgrammerError("Unknown product type");
  }
};

const eventDefinitions = [
  "event Transfer(address indexed from, address indexed to, uint256 amount)",
  "event Staked(address indexed user, uint256 amount)",
  "event Withdrawn(address indexed user, uint256 amount)",
] as const;
interface BaseLog {
  address: string;
  blockNumber: number;
  removed: boolean;
  transactionHash: string;
  logIndex: number;
}
type LogTransfer = BaseLog & {
  eventName: "Transfer";
  args: {
    from: string;
    to: string;
    amount: BigInt;
  };
};
type LogStaked = BaseLog & {
  eventName: "Staked";
  args: {
    user: string;
    amount: BigInt;
  };
};
type LogWithdrawn = BaseLog & {
  eventName: "Withdrawn";
  args: {
    user: string;
    amount: BigInt;
  };
};
type Log = LogTransfer | LogStaked | LogWithdrawn;
const eventConfigs = eventDefinitions.map((eventDefinition) => ({
  selector: getEventSelector(eventDefinition),
  abi: parseAbi([eventDefinition]),
}));
const eventConfigsByTopic = keyBy(eventConfigs, (c) => c.selector);

const ethersInterface = new ethers.utils.Interface(eventDefinitions);

const parseRawLog = (log: ethers.Event | ethers.providers.Log): Log => ({
  address: log.address,
  blockNumber: log.blockNumber,
  removed: log.removed,
  transactionHash: log.transactionHash,
  logIndex: log.logIndex,
  ...(decodeEventLog({
    abi: eventConfigsByTopic[log.topics[0]].abi,
    data: log.data as Hex,
    topics: log.topics as any,
  }) as any),
});

const filterLogByTypeOfProduct = (log: Log, product: DbBeefyProduct) => {
  if (isBeefyBoost(product)) {
    return log.eventName === "Staked" || log.eventName === "Withdrawn";
  } else if (isBeefyGovVault(product)) {
    return log.eventName === "Staked" || log.eventName === "Withdrawn";
  } else if (isBeefyStandardVault(product)) {
    return log.eventName === "Transfer";
  } else {
    throw new ProgrammerError("Unknown product type");
  }
};

const logToTransfers = (log: Log, { chain, decimals }: { chain: Chain; decimals: number }): ERC20Transfer[] => {
  const valueMultiplier = new Decimal(10).pow(-decimals);
  const baseTransfer = {
    blockNumber: log.blockNumber,
    chain: chain,
    logLineage: "rpc" as const,
    logIndex: log.logIndex,
    tokenAddress: normalizeAddressOrThrow(log.address),
    tokenDecimals: decimals,
    transactionHash: log.transactionHash,
  };
  const value = valueMultiplier.mul(log.args.amount.toString());
  if (log.eventName === "Transfer") {
    return [
      { ...baseTransfer, amountTransferred: value.negated(), ownerAddress: log.args.from },
      { ...baseTransfer, amountTransferred: value, ownerAddress: log.args.to },
    ];
  } else if (log.eventName === "Staked") {
    return [{ ...baseTransfer, amountTransferred: value, ownerAddress: log.args.user }];
  } else if (log.eventName === "Withdrawn") {
    return [{ ...baseTransfer, amountTransferred: value.negated(), ownerAddress: log.args.user }];
  } else {
    throw new ProgrammerError("Event type not handled: " + log);
  }
};
