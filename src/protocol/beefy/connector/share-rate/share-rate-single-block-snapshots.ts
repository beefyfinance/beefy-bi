import Decimal from "decimal.js";
import { ethers } from "ethers";
import * as Rx from "rxjs";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, isInRange } from "../../../../utils/range";
import { forkOnNullableField$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { callLockProtectedRpc } from "../../../../utils/shared-resources/shared-rpc";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { fetchBlock$ } from "../../../common/loader/blocks";
import { DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx, Throwable } from "../../../common/types/import-context";
import { extractObjsAndRangeFromOptimizerOutput, isAddressBatchQueries, isJsonRpcBatchQueries } from "../../../common/utils/query/optimizer-utils";
import { AddressBatchOutput, JsonRpcBatchOutput, QueryOptimizerOutput } from "../../../common/utils/query/query-types";
import { fetchSingleBeefyProductShareRateAndDatetime$ } from "./share-rate-multi-block";
import {
  BeefyShareRateBatchCallResult,
  extractRawPpfsFromFunctionResult,
  getBlockTag,
  getCallParamsFromProductAndRange,
  ppfsToVaultSharesRate,
  rangeToBlockNumber,
} from "./share-rate-utils";

const logger = rootLogger.child({ module: "beefy", component: "share-rate-snapshots" });

/**
 * This is optimized for fetching queries returned by the optimizer
 * Mostly used by the share rate snapshots so we can multicall multiple vaults ppfs at once
 */
type MultipleShareRateResult = { product: DbBeefyStdVaultProduct; result: BeefyShareRateBatchCallResult };
export function fetchMultipleShareRate$<
  TObj,
  TQueryContent extends { product: DbBeefyStdVaultProduct },
  TErr extends ErrorEmitter<TObj>,
  TRes,
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getCallParams: (obj: TObj) => QueryOptimizerOutput<TQueryContent, number>;
  formatOutput: (obj: TObj, results: MultipleShareRateResult[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const fetchJsonRpcBatch$: Rx.OperatorFunction<
    { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number> },
    {
      obj: TObj;
      query: JsonRpcBatchOutput<TQueryContent, number>;
      results: MultipleShareRateResult[];
    }
  > = Rx.pipe(
    fetchSingleBeefyProductShareRateAndDatetime$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      getCallParams: (item) => ({
        type: "fetch",
        ...getCallParamsFromProductAndRange(item.query.obj.product, item.query.range),
      }),
      formatOutput: (item, result) => ({
        ...item,
        results: [{ product: item.query.obj.product, result }],
      }),
    }),
  );

  const logInfos = { msg: "Fetching Beefy share rate address batch mode", data: { chain: options.ctx.chain } };
  const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
  const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;

  type ResultWithMabeBlockDatetime = { shareRate: Decimal; product: DbBeefyStdVaultProduct; blockNumber: number; blockDatetime: Date | null };
  const getLogsAddressBatch$: Rx.OperatorFunction<
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number> },
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number>; results: MultipleShareRateResult[] }
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

      let calls = item.query.objs.map((obj) => ({
        allowFailure: true,
        callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
        target: getCallParamsFromProductAndRange(obj.product, item.query.range).vaultAddress,
      }));

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

        const results: ResultWithMabeBlockDatetime[] = [];
        for (let rIdx = 0; rIdx < item.query.objs.length; rIdx++) {
          const product = item.query.objs[rIdx].product;
          const param = getCallParamsFromProductAndRange(product, item.query.range);
          const ppfsData = multicallResults[rIdx];
          // if the call failed, we immediately know it was an empty vault, we don't get the error back anyway
          let shareRate = new Decimal("1");
          if (ppfsData.success) {
            const rawPpfs = extractRawPpfsFromFunctionResult(
              BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", ppfsData.returnData),
            ) as ethers.BigNumber;
            shareRate = ppfsToVaultSharesRate(param.vaultDecimals, param.underlyingDecimals, rawPpfs);
          }

          results.push({ product, blockNumber: blockTag, blockDatetime, shareRate });
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
        (res): MultipleShareRateResult => ({
          product: res.product,
          result: {
            blockDatetime: item.blockDatetime,
            blockNumber: item.blockNumber,
            shareRate: res.shareRate,
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
    Rx.map(({ obj, results }) => options.formatOutput(obj, results)),
  );
}

export function extractShareRateFromOptimizerOutput<TObj>(
  output: QueryOptimizerOutput<TObj, number>,
  getProduct: (obj: TObj) => DbBeefyStdVaultProduct,
  shareRateResults: MultipleShareRateResult[],
): { obj: TObj; range: Range<number>; result: BeefyShareRateBatchCallResult }[] {
  return extractObjsAndRangeFromOptimizerOutput({ output, objKey: (o) => getProduct(o).productKey }).flatMap(({ obj, range }) => {
    const product = getProduct(obj);
    const results = shareRateResults.filter((p) => p.product.productKey === product.productKey && isInRange(range, p.result.blockNumber));
    if (results.length !== 1) {
      throw new ProgrammerError({
        msg: "Not exactly 1 ppfs result for vault",
        data: {
          productKey: product.productKey,
          results: results.map((r) => ({ result: { ...r.result, shareRate: r.result.shareRate.toString() }, productKey: r.product.productKey })),
          shareRateResults: shareRateResults.map((r) => ({
            result: { ...r.result, shareRate: r.result.shareRate.toString() },
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
