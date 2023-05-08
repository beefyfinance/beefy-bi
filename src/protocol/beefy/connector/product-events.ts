import Decimal from "decimal.js";
import { groupBy, keyBy } from "lodash";
import * as Rx from "rxjs";
import { Hex, decodeEventLog, getEventSelector, parseAbi } from "viem";
import { MultiAddressEventFilter } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range, isInRange } from "../../../utils/range";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ERC20Transfer, fetchERC20TransferToAStakingContract$, fetchErc20Transfers$ } from "../../common/connector/erc20-transfers";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct, DbProduct } from "../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import {
  AddressBatchOutput,
  JsonRpcBatchOutput,
  QueryOptimizerOutput,
  isAddressBatchQueries,
  isJsonRpcBatchQueries,
} from "../../common/utils/optimize-range-queries";
import { getProductContractAddress } from "../utils/contract-accessors";
import { isBeefyBoost, isBeefyGovVault, isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "product-events" });

export function fetchProductEvents$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getCallParams: (obj: TObj) => QueryOptimizerOutput<{ product: DbBeefyProduct }, number>;
  formatOutput: (obj: TObj, transfers: ERC20Transfer[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const fetchJsonRpcBatch$: Rx.OperatorFunction<
    { obj: TObj; batch: JsonRpcBatchOutput<{ product: DbBeefyProduct }, number> },
    { obj: TObj; product: DbBeefyProduct; range: Range<number>; transfers: ERC20Transfer[] }
  > = Rx.pipe(
    // flatten every query
    Rx.map(({ obj, batch: { queries } }) => queries.map((query) => ({ obj, product: query.obj.product, range: query.range }))),
    Rx.concatAll(),

    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          // set the right product type
          Rx.filter(
            (item): item is { obj: TObj; product: DbBeefyGovVaultProduct | DbBeefyBoostProduct; range: Range<number> } =>
              isBeefyGovVault(item.product) || isBeefyBoost(item.product),
          ),

          fetchERC20TransferToAStakingContract$({
            ctx: options.ctx,
            emitError: (item, report) => options.emitError(item.obj, report),
            getQueryParams: (item) => {
              if (isBeefyBoost(item.product)) {
                const boost = item.product.productData.boost;
                return {
                  tokenAddress: boost.staked_token_address,
                  decimals: boost.staked_token_decimals,
                  trackAddress: boost.contract_address,
                  fromBlock: item.range.from,
                  toBlock: item.range.to,
                };
              } else if (isBeefyGovVault(item.product)) {
                // for gov vaults we don't have a share token so we use the underlying token
                // transfers and filter on those transfer from and to the contract address
                const vault = item.product.productData.vault;
                return {
                  tokenAddress: vault.want_address,
                  decimals: vault.want_decimals,
                  trackAddress: vault.contract_address,
                  fromBlock: item.range.from,
                  toBlock: item.range.to,
                };
              } else {
                throw new ProgrammerError({ msg: "Invalid product type, should be gov vault or boost", data: item });
              }
            },
            formatOutput: (item, transfers) => ({ ...item, transfers }),
          }),
        ),
        items$.pipe(
          // set the right product type
          Rx.filter((item): item is { obj: TObj; product: DbBeefyStdVaultProduct; range: Range<number> } => isBeefyStandardVault(item.product)),

          // fetch the vault transfers
          fetchErc20Transfers$({
            ctx: options.ctx,
            emitError: (item, report) => options.emitError(item.obj, report),
            getQueryParams: (item) => {
              const vault = item.product.productData.vault;
              return {
                tokenAddress: vault.contract_address,
                decimals: vault.token_decimals,
                fromBlock: item.range.from,
                toBlock: item.range.to,
              };
            },
            formatOutput: (item, transfers) => ({ ...item, transfers }),
          }),
        ),
      ),
    ),
  );

  const getLogsAddressBatch$: Rx.OperatorFunction<
    { obj: TObj; batch: AddressBatchOutput<{ product: DbBeefyProduct }, number> },
    { obj: TObj; product: DbBeefyProduct; range: Range<number>; transfers: ERC20Transfer[] }
  > = Rx.pipe(
    // flatten every query
    Rx.map(({ obj, batch: { queries } }) => queries.map((query) => ({ obj, query }))),
    Rx.concatAll(),

    Rx.mergeMap(async (item) => {
      const provider = options.ctx.rpcConfig.linearProvider;
      const products = item.query.objs.map((o) => o.product);
      const adresses = products.map(getProductContractAddress);
      const filter: MultiAddressEventFilter = {
        address: adresses,
        topics: [eventConfigs.map((e) => e.selector)],
        fromBlock: item.query.range.from,
        toBlock: item.query.range.to,
      };

      const rawLogs = await callLockProtectedRpc(() => provider.getLogsMultiAddress(filter), {
        chain: options.ctx.chain,
        rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
        logInfos: { msg: "Fetching product events with address batch", data: item },
        maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
        noLockIfNoLimit: true,
        provider: options.ctx.rpcConfig.linearProvider,
      });

      const logs = rawLogs
        .map(
          (log): Log => ({
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
          }),
        )
        .filter((log) => !log.removed);

      // decode logs to transfer events
      const productByAddress = keyBy(products, (p) => getProductContractAddress(p).toLocaleLowerCase());
      const postFiltersByAddress = keyBy(item.query.postFilters, (f) => getProductContractAddress(f.obj.product).toLocaleLowerCase());
      const transfers = logs
        .flatMap((log): ERC20Transfer[] => {
          const product = productByAddress[log.address.toLocaleLowerCase()];
          const address = getProductContractAddress(product);
          const decimals = getProductDecimals(product);
          const valueMultiplier = new Decimal(10).pow(-decimals);
          const baseTransfer = {
            blockNumber: log.blockNumber,
            chain: options.ctx.chain,
            logLineage: "rpc" as const,
            logIndex: log.logIndex,
            tokenAddress: address,
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
        })
        // apply postfilters
        .filter((transfer) => {
          const postFilter = postFiltersByAddress[transfer.tokenAddress.toLocaleLowerCase()];
          if (postFilter.filter === "no-filter") {
            return true;
          }
          return postFilter.filter.some((r) => isInRange(r, transfer.blockNumber));
        });

      const transferByAddress = groupBy(transfers, (t) => t.tokenAddress.toLocaleLowerCase());

      return item.query.objs.map(({ product }) => ({
        obj: item.obj,
        product,
        range: item.query.range,
        transfers: transferByAddress[getProductContractAddress(product).toLocaleLowerCase()] || [],
      }));
    }, options.ctx.streamConfig.workConcurrency),

    Rx.concatAll(),
  );

  return Rx.pipe(
    // wrap obj
    Rx.map((obj: TObj) => ({ obj, batch: options.getCallParams(obj) })),

    // handle different types of requests differently
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; batch: JsonRpcBatchOutput<{ product: DbBeefyProduct }, number> } =>
            isJsonRpcBatchQueries(item.batch),
          ),
          fetchJsonRpcBatch$,
        ),
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; batch: AddressBatchOutput<{ product: DbBeefyProduct }, number> } =>
            isAddressBatchQueries(item.batch),
          ),
          getLogsAddressBatch$,
        ),
      ),
    ),
    Rx.map(({ obj, transfers }) => options.formatOutput(obj, transfers)),
  );
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
