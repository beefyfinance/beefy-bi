import Decimal from "decimal.js";
import { ethers } from "ethers";
import * as Rx from "rxjs";
import { BeefyVaultV6AbiInterface, ERC20AbiInterface, Multicall3AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range } from "../../../utils/range";
import { forkOnNullableField$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { fetchBlockDatetime$ } from "../../common/connector/block-datetime";
import { fetchBlock$ } from "../../common/loader/blocks";
import { DbBeefyStdVaultProduct } from "../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx, Throwable } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import { extractObjsAndRangeFromOptimizerOutput, isAddressBatchQueries, isJsonRpcBatchQueries } from "../../common/utils/query/optimizer-utils";
import { AddressBatchOutput, JsonRpcBatchOutput, QueryOptimizerOutput } from "../../common/utils/query/query-types";
import { extractRawPpfsFromFunctionResult, getBlockTag, ppfsToVaultSharesRate, rangeToBlockNumber } from "./share-rate/share-rate-utils";

const logger = rootLogger.child({ module: "beefy", component: "product-statistics-snapshots" });

interface BeefyProductStatisticsCallParams {
  vaultAddress: string;
  vaultDecimals: number;
  underlyingAddress: string;
  underlyingDecimals: number;
  blockNumber: number;
}

interface BeefyProductStatisticsBatchCallResult {
  vaultTotalSupply: Decimal;
  vaultShareRate: Decimal;
  stakedUnderlying: Decimal;
  underlyingTotalSupply: Decimal;
  blockNumber: number;
  blockDatetime: Date;
}

function getCallParamsFromProductAndRange(product: DbBeefyStdVaultProduct, range: Range<number>): BeefyProductStatisticsCallParams {
  const vault = product.productData.vault;
  return {
    underlyingAddress: vault.want_address,
    underlyingDecimals: vault.want_decimals,
    vaultAddress: vault.contract_address,
    vaultDecimals: vault.token_decimals,
    blockNumber: rangeToBlockNumber(range),
  };
}

/**
 * This is optimized for fetching queries returned by the optimizer
 * Mostly used by the product stats snapshots so we can multicall multiple vaults ppfs at once
 */
type MultipleProductStatisticsResult = { product: DbBeefyStdVaultProduct; result: BeefyProductStatisticsBatchCallResult };
export function fetchMultipleProductStatistics$<
  TObj,
  TQueryContent extends { product: DbBeefyStdVaultProduct },
  TErr extends ErrorEmitter<TObj>,
  TRes,
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getCallParams: (obj: TObj) => QueryOptimizerOutput<TQueryContent, number>;
  formatOutput: (obj: TObj, results: MultipleProductStatisticsResult[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const fetchJsonRpcBatch$: Rx.OperatorFunction<
    { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number> },
    {
      obj: TObj;
      query: JsonRpcBatchOutput<TQueryContent, number>;
      results: MultipleProductStatisticsResult[];
    }
  > = Rx.pipe(
    // we also need the date of each block
    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      getBlockNumber: (item) => getBlockTag(options.ctx, getCallParamsFromProductAndRange(item.query.obj.product, item.query.range).blockNumber),
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),
    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      logInfos: { msg: "fetching product statistics using jsonrpc batch" },
      rpcCallsPerInputObj: {
        eth_call: 4,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: (item) => getCallParamsFromProductAndRange(item.query.obj.product, item.query.range),
      processBatch: async (provider, params) => {
        // fetch all ppfs in one go, this will batch calls using jsonrpc batching
        type ProductStatsEntry = [
          BeefyProductStatisticsCallParams,
          {
            vaultPpfs: ethers.BigNumber;
            vaultTotalSupply: ethers.BigNumber;
            stakedUnderlying: ethers.BigNumber;
            underlyingTotalSupply: ethers.BigNumber;
          },
        ];
        type MapEntry = [
          BeefyProductStatisticsCallParams,
          {
            vaultShareRate: Decimal;
            vaultTotalSupply: Decimal;
            stakedUnderlying: Decimal;
            underlyingTotalSupply: Decimal;
            blockNumber: number;
          },
        ];
        type ErrEntry = [BeefyProductStatisticsCallParams, ErrorReport];
        type CallResult<T> = { type: "success"; data: T } | { type: "error"; data: ErrEntry };
        let productStatisticsPromises: Promise<CallResult<MapEntry>>[] = [];
        for (const contractCall of params) {
          const blockTag = getBlockTag(options.ctx, contractCall.blockNumber);
          const vaultContract = new ethers.Contract(contractCall.vaultAddress, BeefyVaultV6AbiInterface, provider);
          const underlyingContract = new ethers.Contract(contractCall.underlyingAddress, ERC20AbiInterface, provider);

          const isErroringContractCall =
            options.ctx.chain === "ethereum" &&
            contractCall.vaultAddress.toLocaleLowerCase() === "0x5da90ba82bed0ab701e6762d2bf44e08634d9776" &&
            contractCall.blockNumber === 19384744;

          const rawPromise = Promise.all([
            vaultContract.callStatic.getPricePerFullShare({ blockTag }) as Promise<[ethers.BigNumber]>,
            vaultContract.callStatic.totalSupply({ blockTag }) as Promise<[ethers.BigNumber]>,
            isErroringContractCall
              ? Promise.resolve([ethers.BigNumber.from(0)])
              : (vaultContract.callStatic.balance({ blockTag }) as Promise<[ethers.BigNumber]>),
            underlyingContract.callStatic.totalSupply({ blockTag }) as Promise<[ethers.BigNumber]>,
          ]);

          const shareRatePromise = rawPromise
            .then(
              (callResults): CallResult<ProductStatsEntry> => ({
                type: "success",
                data: [
                  contractCall,
                  {
                    vaultPpfs: extractRawPpfsFromFunctionResult(callResults[0]),
                    vaultTotalSupply: callResults[1][0],
                    stakedUnderlying: callResults[2][0],
                    underlyingTotalSupply: callResults[3][0],
                  },
                ] as ProductStatsEntry,
              }),
            )
            .then((res): CallResult<MapEntry> => {
              if (res.type === "success") {
                return {
                  type: "success",
                  data: [
                    contractCall,
                    {
                      vaultShareRate: ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, res.data[1].vaultPpfs),
                      vaultTotalSupply: new Decimal(res.data[1].vaultTotalSupply.toString()),
                      stakedUnderlying: new Decimal(res.data[1].stakedUnderlying.toString()),
                      underlyingTotalSupply: new Decimal(res.data[1].underlyingTotalSupply.toString()),
                      blockNumber: blockTag,
                    } as BeefyProductStatisticsBatchCallResult,
                  ],
                };
              } else if (res.type === "error") {
                return res;
              } else {
                throw new ProgrammerError({ msg: "Unmapped type", data: { res } });
              }
            });

          productStatisticsPromises.push(shareRatePromise);
        }

        const batchResults = await Promise.all(productStatisticsPromises);
        return {
          successes: new Map(batchResults.filter((r): r is { type: "success"; data: MapEntry } => r.type === "success").map((r) => r.data)),
          errors: new Map(batchResults.filter((r): r is { type: "error"; data: ErrEntry } => r.type === "error").map((r) => r.data)),
        };
      },
      formatOutput: (item, productStatistics) => ({
        ...item,
        results: [{ product: item.query.obj.product, result: { ...productStatistics, blockDatetime: item.blockDatetime } }],
      }),
    }),
  );

  const logInfos = { msg: "Fetching Beefy product statistics address batch mode", data: { chain: options.ctx.chain } };
  const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
  const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;

  type ResultWithMaybeBlockDatetime = {
    vaultTotalSupply: Decimal;
    vaultShareRate: Decimal;
    stakedUnderlying: Decimal;
    underlyingTotalSupply: Decimal;
    product: DbBeefyStdVaultProduct;
    blockNumber: number;
    blockDatetime: Date | null;
  };
  const getLogsAddressBatch$: Rx.OperatorFunction<
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number> },
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number>; results: MultipleProductStatisticsResult[] }
  > = Rx.pipe(
    Rx.tap((item) => logger.trace({ msg: "item", data: item })),

    Rx.mergeMap(async (item) => {
      const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
      if (!mcMap) {
        throw new ProgrammerError({
          msg: "Cannot use this method, multicall contract is not defined for this chain",
          data: { chain: options.ctx.chain },
        });
      }

      const blockTag = getBlockTag(options.ctx, rangeToBlockNumber(item.query.range));
      const provider = options.ctx.rpcConfig.linearProvider;

      const multicallAddress = mcMap.multicallAddress;
      const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);

      let calls = item.query.objs.flatMap((obj) => {
        const callParams = getCallParamsFromProductAndRange(obj.product, item.query.range);
        return [
          {
            allowFailure: true,
            callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
            target: callParams.vaultAddress,
          },
          {
            allowFailure: false,
            callData: BeefyVaultV6AbiInterface.encodeFunctionData("totalSupply"),
            target: callParams.vaultAddress,
          },
          {
            allowFailure: false,
            callData: BeefyVaultV6AbiInterface.encodeFunctionData("balance"),
            target: callParams.vaultAddress,
          },
          {
            allowFailure: false,
            callData: ERC20AbiInterface.encodeFunctionData("totalSupply"),
            target: callParams.underlyingAddress,
          },
        ];
      });

      if (options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
        calls.push({
          allowFailure: false,
          callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
          target: multicallAddress,
        });
      }

      try {
        const multicallResults: { success: boolean; returnData: string }[] = await callLockProtectedRpc(
          () => multicallContract.callStatic.aggregate3(calls, { blockTag }),
          {
            chain: options.ctx.chain,
            rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
            logInfos: mergeLogsInfos({ msg: "Fetching multicall ppfs", data: item }, logInfos),
            maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
            noLockIfNoLimit: true,
            provider: options.ctx.rpcConfig.linearProvider,
          },
        );

        let blockDatetime: Date | null = null;
        if (options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
          const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult(
            "getCurrentBlockTimestamp",
            multicallResults[multicallResults.length - 1].returnData,
          )[0] as ethers.BigNumber;
          blockDatetime = new Date(blockTimestamp.toNumber() * 1000);
        }

        const results: ResultWithMaybeBlockDatetime[] = [];
        for (let pIdx = 0; pIdx < item.query.objs.length; pIdx++) {
          const product = item.query.objs[pIdx].product;
          let rIdx = pIdx * 4;

          const param = getCallParamsFromProductAndRange(product, item.query.range);
          const ppfsData = multicallResults[rIdx++];
          // if the call failed, we immediately know it was an empty vault, we don't get the error back anyway
          let vaultShareRate = new Decimal("1");
          if (ppfsData.success) {
            const rawPpfs = extractRawPpfsFromFunctionResult(
              BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", ppfsData.returnData),
            ) as ethers.BigNumber;
            vaultShareRate = ppfsToVaultSharesRate(param.vaultDecimals, param.underlyingDecimals, rawPpfs);
          }

          const totalSupplyData = multicallResults[rIdx++];
          const vaultTotalSupply = new Decimal(
            (ERC20AbiInterface.decodeFunctionResult("totalSupply", totalSupplyData.returnData)[0] as ethers.BigNumber).toString(),
          );
          const stakedUnderlyingData = multicallResults[rIdx++];
          const stakedUnderlying = new Decimal(
            (BeefyVaultV6AbiInterface.decodeFunctionResult("balance", stakedUnderlyingData.returnData)[0] as ethers.BigNumber).toString(),
          );
          const underlyingTotalSupplyData = multicallResults[rIdx++];
          const underlyingTotalSupply = new Decimal(
            (ERC20AbiInterface.decodeFunctionResult("totalSupply", underlyingTotalSupplyData.returnData)[0] as ethers.BigNumber).toString(),
          );

          results.push({
            product,
            blockNumber: blockTag,
            blockDatetime,
            vaultShareRate,
            vaultTotalSupply,
            stakedUnderlying,
            underlyingTotalSupply,
          });
        }

        return [{ ...item, results, blockNumber: blockTag, blockDatetime }];
      } catch (e: unknown) {
        const error = e as Throwable;
        // here, none of the retrying worked, so we emit all the objects as in error
        const report: ErrorReport = {
          error,
          infos: mergeLogsInfos({ msg: "Error during address batch query multicall", data: { chain: options.ctx.chain, err: error } }, logInfos),
        };
        logger.debug(report.infos);
        logger.debug(report.error);
        options.emitError(item.obj, report);
        return Rx.EMPTY;
      }
    }, workConcurrency),
    Rx.concatAll(),

    Rx.tap((item) => item),

    // handle those chains where multicall is not able to fetch the datetime
    forkOnNullableField$({
      key: "blockDatetime",
      handleNulls$: Rx.pipe(
        Rx.tap((item) =>
          logger.trace({ msg: "Could not use Multicall3.getCurrentBlockTimestamp, fetching the proper timestamp", data: { mcMap, item } }),
        ),
        fetchBlockDatetime$({
          ctx: options.ctx,
          emitError: (item, errReport) => options.emitError(item.obj, errReport),
          getBlockNumber: (item) => item.blockNumber,
          formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
        }),
      ),
      // sometimes the multicall result differ from other methods so if the block exists we
      // want to use the same datetime we already used in the database to avoid duplicate rows
      handleNonNulls$: Rx.pipe(
        fetchBlock$({
          ctx: options.ctx,
          chain: options.ctx.chain,
          emitError: (item, errReport) => options.emitError(item.obj, errReport),
          getBlockNumber: (item) => item.blockNumber,
          formatOutput: (item, block) => ({
            ...item,
            blockDatetime: block ? block.datetime : item.blockDatetime,
          }),
        }),
      ),
    }),

    Rx.map((item) => ({
      obj: item.obj,
      query: item.query,
      results: item.results.map(
        (res): MultipleProductStatisticsResult => ({
          product: res.product,
          result: {
            blockDatetime: item.blockDatetime,
            blockNumber: item.blockNumber,
            vaultShareRate: res.vaultShareRate,
            vaultTotalSupply: res.vaultTotalSupply,
            stakedUnderlying: res.stakedUnderlying,
            underlyingTotalSupply: res.underlyingTotalSupply,
          },
        }),
      ),
    })),
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

    // apply decimals to the results
    Rx.map(({ obj, query, results }) => ({
      obj,
      query,
      results: results.map((res) => ({
        product: res.product,
        result: {
          ...res.result,
          vaultTotalSupply: res.result.vaultTotalSupply.div(new Decimal(10).pow(res.product.productData.vault.token_decimals)),
          stakedUnderlying: res.result.stakedUnderlying.div(new Decimal(10).pow(res.product.productData.vault.want_decimals)),
          underlyingTotalSupply: res.result.underlyingTotalSupply.div(new Decimal(10).pow(res.product.productData.vault.want_decimals)),
        },
      })),
    })),

    Rx.map(({ obj, results }) => options.formatOutput(obj, results)),
  );
}

export function extractProductStatisticsFromOptimizerOutput<TObj>(
  output: QueryOptimizerOutput<TObj, number>,
  getProduct: (obj: TObj) => DbBeefyStdVaultProduct,
  productStatsResults: MultipleProductStatisticsResult[],
): { obj: TObj; range: Range<number>; result: BeefyProductStatisticsBatchCallResult }[] {
  return extractObjsAndRangeFromOptimizerOutput({ output, objKey: (o) => getProduct(o).productKey }).flatMap(({ obj, range }) => {
    const product = getProduct(obj);
    const results = productStatsResults.filter((p) => p.product.productKey === product.productKey);
    if (results.length !== 1) {
      throw new ProgrammerError({
        msg: "Not exactly 1 ppfs result for vault",
        data: {
          productKey: product.productKey,
          range,
          output,
          results: results.map((r) => ({
            result: {
              ...r.result,
              vaultShareRate: r.result.vaultShareRate.toString(),
              vaultTotalSupply: r.result.vaultTotalSupply.toString(),
              stakedUnderlying: r.result.stakedUnderlying.toString(),
              underlyingTotalSupply: r.result.underlyingTotalSupply.toString(),
            },
            productKey: r.product.productKey,
          })),
          productStatsResults: productStatsResults.map((r) => ({
            result: {
              ...r.result,
              vaultShareRate: r.result.vaultShareRate.toString(),
              vaultTotalSupply: r.result.vaultTotalSupply.toString(),
              stakedUnderlying: r.result.stakedUnderlying.toString(),
              underlyingTotalSupply: r.result.underlyingTotalSupply.toString(),
            },
            productKey: r.product.productKey,
          })),
        },
      });
    }
    return {
      obj,
      range,
      result: results[0].result,
    };
  });
}
