import Decimal from "decimal.js";
import { ethers } from "ethers";
import { get, isArray, min } from "lodash";
import * as Rx from "rxjs";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range, isInRange } from "../../../utils/range";
import { forkOnNullableField$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { fetchBlockDatetime$ } from "../../common/connector/block-datetime";
import { fetchBlock$ } from "../../common/loader/blocks";
import { DbBeefyStdVaultProduct } from "../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx, Throwable } from "../../common/types/import-context";
import { RPCBatchCallResult, batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import {
  AddressBatchOutput,
  JsonRpcBatchOutput,
  QueryOptimizerOutput,
  extractObjsAndRangeFromOptimizerOutput,
  isAddressBatchQueries,
  isJsonRpcBatchQueries,
} from "../../common/utils/optimize-range-queries";

const logger = rootLogger.child({ module: "beefy", component: "share-rate" });

/**
 * This is for fetching multiple share rate with distinct block number
 * While fetching investments for ex.
 * This is not efficient for ppfs snapshoting where we can do way better
 */
export function fetchSingleBeefyProductShareRateAndDatetime$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  geCallParams: (obj: TObj) => (BeefyShareRateCallParams & { type: "fetch" }) | { type: "set-to-1"; blockNumber: number };
  formatOutput: (obj: TObj, result: BeefyShareRateBatchCallResult) => TRes;
}) {
  const logInfos = { msg: "Fetching Beefy share rate single mode", data: { chain: options.ctx.chain } };
  const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
  type MaybeResult = { result: { blockNumber: number; shareRate: Decimal; blockDatetime: Date } | null };

  const noMulticallPipeline = Rx.pipe(
    Rx.tap(({ obj }: { obj: TObj; param: BeefyShareRateCallParams }) =>
      logger.trace(mergeLogsInfos({ msg: "no multicall", data: { transferData: obj } }, logInfos)),
    ),

    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      logInfos: mergeLogsInfos({ msg: "maybe using multicall" }, logInfos),
      rpcCallsPerInputObj: {
        eth_call: 1,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: (item) => item.param,
      processBatch: async (provider, params) => {
        // fetch all ppfs in one go, this will batch calls using jsonrpc batching
        type PPFSEntry = [BeefyShareRateCallParams, ethers.BigNumber];
        type MapEntry = [BeefyShareRateCallParams, Decimal];
        type ErrEntry = [BeefyShareRateCallParams, ErrorReport];
        type CallResult<T> = { type: "success"; data: T } | { type: "error"; data: ErrEntry };
        let shareRatePromises: Promise<CallResult<MapEntry>>[] = [];
        for (const contractCall of params) {
          let rawPromise: Promise<[ethers.BigNumber]>;

          const contract = new ethers.Contract(contractCall.vaultAddress, BeefyVaultV6AbiInterface, provider);
          rawPromise = contract.callStatic.getPricePerFullShare({ blockTag: getBlockTag(options.ctx, contractCall.blockNumber) });

          const shareRatePromise = rawPromise
            .then(
              (ppfs): CallResult<PPFSEntry> => ({
                type: "success",
                data: [contractCall, extractRawPpfsFromFunctionResult(ppfs)] as PPFSEntry,
              }),
            )
            // empty vaults WILL throw an error
            .catch((err): CallResult<PPFSEntry> => {
              if (isEmptyVaultPPFSError(err)) {
                return { type: "success", data: [contractCall, ethers.BigNumber.from(0)] as PPFSEntry };
              } else {
                return {
                  type: "error",
                  data: [contractCall, { error: err, infos: { msg: "Unrecoverable error while fetching ppfs", data: { contractCall } } }] as ErrEntry,
                };
              }
            })
            .then((res): CallResult<MapEntry> => {
              if (res.type === "success") {
                const vaultShareRate = ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, res.data[1]);
                return { type: "success", data: [contractCall, vaultShareRate] };
              } else if (res.type === "error") {
                return res;
              } else {
                throw new ProgrammerError({ msg: "Unmapped type", data: { res } });
              }
            });

          shareRatePromises.push(shareRatePromise);
        }

        const batchResults = await Promise.all(shareRatePromises);
        return {
          successes: new Map(batchResults.filter((r): r is { type: "success"; data: MapEntry } => r.type === "success").map((r) => r.data)),
          errors: new Map(batchResults.filter((r): r is { type: "error"; data: ErrEntry } => r.type === "error").map((r) => r.data)),
        };
      },
      formatOutput: (item, shareRate) => ({ ...item, shareRate, blockNumber: getBlockTag(options.ctx, item.param.blockNumber) }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      getBlockNumber: (item) => item.param.blockNumber,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),
  );

  const multicallPipeline = Rx.pipe(
    Rx.tap(({ obj }: { obj: TObj; param: BeefyShareRateCallParams }) =>
      logger.trace(mergeLogsInfos({ msg: "maybe using multicall", data: { transferData: obj } }, logInfos)),
    ),

    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      logInfos: mergeLogsInfos({ msg: "maybe using multicall" }, logInfos),
      rpcCallsPerInputObj: {
        // this should be 1 but it's an heavy call so we virtually add 1 so batches are smaller
        // this lowers the probability that the RPC will timeout
        eth_call: 2,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: ({ param }) => param,
      processBatch: async (provider, params): Promise<RPCBatchCallResult<BeefyShareRateCallParams, MaybeResult>> => {
        // find out if we can use multicall
        const minBlockNumber = min(params.map((c) => c.blockNumber)) || 0;
        if (!mcMap || mcMap.createdAtBlock >= minBlockNumber) {
          // we couldn't do multicall so we handle it later on
          logger.trace(mergeLogsInfos({ msg: "Could not use multicall, fallback to batch by call type", data: { mcMap, minBlockNumber } }, logInfos));
          return {
            successes: new Map(params.map((param) => [param, { result: null }] as const)),
            errors: new Map(),
          };
        }

        const multicallAddress = mcMap.multicallAddress;
        const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);

        try {
          const batch = params.map((param) => {
            // read the next block for those chains who can't read their own writes
            const blockTag = getBlockTag(options.ctx, param.blockNumber);
            const calls = [
              {
                allowFailure: true,
                callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
                target: param.vaultAddress,
              },
              {
                allowFailure: false,
                callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
                target: multicallAddress,
              },
            ];
            return multicallContract.callStatic.aggregate3(calls, { blockTag });
          });

          const result: { success: boolean; returnData: string }[][] = await Promise.all(batch);

          const successResults = params.map((param, i) => {
            const r = result[i];

            let rIdx = 0;

            const ppfsData = r[rIdx++];
            // if the call failed, we immediately know it was an empty vault, we don't get the error back anyway
            let shareRate = new Decimal("1");
            if (ppfsData.success) {
              const rawPpfs = extractRawPpfsFromFunctionResult(
                BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", ppfsData.returnData),
              ) as ethers.BigNumber;
              shareRate = ppfsToVaultSharesRate(param.vaultDecimals, param.underlyingDecimals, rawPpfs);
            }

            let blockDatetime: Date | null = null;
            const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult(
              "getCurrentBlockTimestamp",
              r[rIdx++].returnData,
            )[0] as ethers.BigNumber;
            blockDatetime = new Date(blockTimestamp.toNumber() * 1000);

            return [param, { result: { blockNumber: getBlockTag(options.ctx, param.blockNumber), blockDatetime, shareRate } }] as const;
          });

          return {
            successes: new Map(successResults),
            errors: new Map(),
          };
        } catch (err: any) {
          logger.error(mergeLogsInfos({ msg: "Error fetching transfer data with custom batch builder", data: { err } }, logInfos));
          logger.error(err);
          const report: ErrorReport = { error: err, infos: { msg: "Error fetching transfer data with custom batch builder" } };

          return {
            successes: new Map(),
            errors: new Map(params.map((p) => [p, report] as const)),
          };
        }
      },
      formatOutput: (item, res) => ({ ...item, ...res }),
    }),

    // handle those calls where multicall is available but was not created yet
    forkOnNullableField$({
      key: "result",
      handleNulls$: Rx.pipe(
        Rx.tap((item) =>
          logger.trace(mergeLogsInfos({ msg: "Could not use multicall, fallback to no-multicall method", data: { mcMap, item } }, logInfos)),
        ),
        Rx.map((item) => ({ obj: item.obj, param: item.param })),
        noMulticallPipeline,
      ),
      handleNonNulls$: Rx.pipe(Rx.map(({ obj, param, result }) => ({ obj, param, ...result }))),
    }),
    Rx.map((item) => options.formatOutput(item.obj, { blockNumber: item.blockNumber, blockDatetime: item.blockDatetime, shareRate: item.shareRate })),
  );

  let strategy = multicallPipeline;
  // if there is no multicall or if multicall has broken blockTimestamp, use no multicall
  if (!mcMap || !options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
    strategy = Rx.pipe(
      noMulticallPipeline,
      Rx.map((item) =>
        options.formatOutput(item.obj, { blockNumber: item.blockNumber, blockDatetime: item.blockDatetime, shareRate: item.shareRate }),
      ),
    );
  }

  return Rx.pipe(
    Rx.tap((obj: TObj) => logger.trace(mergeLogsInfos({ msg: "forking on product type", data: { transferData: obj } }, logInfos))),
    Rx.map((obj) => ({ obj, param: options.geCallParams(obj) })),
    // handle different types of requests differently
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; param: BeefyShareRateCallParams & { type: "fetch" } } => item.param.type === "fetch"),
          strategy,
        ),
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; param: { type: "set-to-1"; blockNumber: number } } => item.param.type === "set-to-1"),
          fetchBlockDatetime$({
            ctx: options.ctx,
            emitError: (item, report) => options.emitError(item.obj, report),
            getBlockNumber: ({ param: { blockNumber } }) => blockNumber,
            formatOutput: (item, blockDatetime) => ({ obj: item.obj, blockNumber: item.param.blockNumber, blockDatetime }),
          }),
          Rx.map((item) =>
            options.formatOutput(item.obj, { blockNumber: item.blockNumber, blockDatetime: item.blockDatetime, shareRate: new Decimal(1) }),
          ),
        ),
      ),
    ),
  );
}

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
      geCallParams: (item) => ({
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
        for (let rIdx = 0; rIdx < calls.length; rIdx++) {
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

const getBlockTag = (ctx: ImportCtx, blockNumber: number): number => {
  // read the next block for those chains who can't read their own writes
  let blockTag = blockNumber;
  if (!ctx.rpcConfig.rpcLimitations.stateChangeReadsOnSameBlock) {
    blockTag = blockNumber + 1;
  }
  return blockTag;
};

export function extractProductShareRateFromOutputAndTransfers<TObj>(
  output: QueryOptimizerOutput<TObj, number>,
  getProduct: (obj: TObj) => DbBeefyStdVaultProduct,
  shareRateResults: MultipleShareRateResult[],
): { obj: TObj; range: Range<number>; shareRate: Decimal; blockNumber: number }[] {
  return extractObjsAndRangeFromOptimizerOutput({ output, objKey: (o) => getProduct(o).productKey }).flatMap(({ obj, range }) => {
    const product = getProduct(obj);
    const results = shareRateResults.filter((p) => p.product.productKey === product.productKey && isInRange(range, p.result.blockNumber));
    if (results.length !== 1) {
      throw new ProgrammerError({
        msg: "Not exactly 1 ppfs result for vault",
        data: { productKey: product.productKey, results: results.map((r) => ({ ...r, vault: r.product.productKey })) },
      });
    }
    return {
      obj,
      range,
      shareRate: results[0].result.shareRate,
      blockNumber: results[0].result.blockNumber,
    };
  });
}

interface BeefyShareRateBatchCallResult {
  shareRate: Decimal;
  blockNumber: number;
  blockDatetime: Date;
}

export interface BeefyShareRateCallParams {
  vaultDecimals: number;
  underlyingDecimals: number;
  vaultAddress: string;
  blockNumber: number;
}

function getCallParamsFromProductAndRange(product: DbBeefyStdVaultProduct, range: Range<number>): BeefyShareRateCallParams {
  const vault = product.productData.vault;
  return {
    underlyingDecimals: vault.want_decimals,
    vaultAddress: vault.contract_address,
    vaultDecimals: vault.token_decimals,
    blockNumber: rangeToBlockNumber(range),
  };
}

function rangeToBlockNumber(range: Range<number>): number {
  const midPoint = Math.round((range.from + range.to) / 2);
  if (!isInRange(range, midPoint)) {
    throw new ProgrammerError({ msg: "Midpoint is not in range, most likely an invalid range", data: { range, midPoint } });
  }
  return midPoint;
}

// sometimes, we get this error: "execution reverted: SafeMath: division by zero"
// this means that the totalSupply is 0 so we set ppfs to zero
export function isEmptyVaultPPFSError(err: any) {
  if (!err) {
    return false;
  }
  const errorMessage = get(err, ["error", "message"]) || get(err, "message") || "";
  return errorMessage.includes("SafeMath: division by zero");
}

export function extractRawPpfsFromFunctionResult<T>(returnData: [T] | T): T {
  // some chains don't return an array (harmony, heco)
  return isArray(returnData) ? returnData[0] : returnData;
}

// takes ppfs and compute the actual rate which can be directly multiplied by the vault balance
// this is derived from mooAmountToOracleAmount in beefy-v2 repo
export function ppfsToVaultSharesRate(mooTokenDecimals: number, depositTokenDecimals: number, ppfs: ethers.BigNumber) {
  const mooTokenAmount = new Decimal("1.0");

  // go to chain representation
  const mooChainAmount = mooTokenAmount.mul(new Decimal(10).pow(mooTokenDecimals)).toDecimalPlaces(0);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.mul(new Decimal(ppfs.toString()));

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount.div(new Decimal(10).pow(mooTokenDecimals + depositTokenDecimals)).toDecimalPlaces(mooTokenDecimals);

  return oracleAmount;
}
