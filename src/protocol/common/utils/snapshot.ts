import Decimal from "decimal.js";
import { ethers } from "ethers";
import * as Rx from "rxjs";
import { Hex, multicall3Abi, parseAbi } from "viem";
import { RpcCallMethod } from "../../../types/rpc-config";
import { Multicall3Abi } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range, SupportedRangeTypes, isInRange } from "../../../utils/range";
import { forkOnNullableField$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../connector/block-datetime";
import { fetchBlock$ } from "../loader/blocks";
import { ErrorEmitter, ErrorReport, ImportCtx, Throwable } from "../types/import-context";
import { RPCBatchCallResult, batchRpcCallsViem$, mergeBatchCallResults } from "./batch-rpc-calls";
import { isAddressBatchQueries, isJsonRpcBatchQueries } from "./query/optimizer-utils";
import { AddressBatchOutput, JsonRpcBatchOutput, QueryOptimizerOutput } from "./query/query-types";
import { CustomViemClient } from "./viem/client";

const logger = rootLogger.child({ module: "beefy", component: "share-rate-snapshots" });

export interface BeefySnapshotCallResult<TRes> {
  res: TRes;
  blockNumber: number;
  blockDatetime: Date;
}

/**
 * This is optimized for fetching queries returned by the optimizer
 * Mostly used by the share rate snapshots so we can multicall multiple vaults ppfs at once
 */
export function singleBlockMulticallSnapshot$<
  TObj,
  TParams,
  TResult,
  TQueryContent,
  TErr extends ErrorEmitter<TObj>,
  TRange extends SupportedRangeTypes,
  TRes,
>(options: {
  ctx: ImportCtx;
  logInfos: LogInfos;
  emitError: TErr;
  getOptimizerQuery: (obj: TObj) => QueryOptimizerOutput<TQueryContent, TRange>;
  getCallParams: (query: TQueryContent, blockNumber: number) => TParams;
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  processBatch: (client: CustomViemClient, batch: TParams[]) => Promise<RPCBatchCallResult<TParams, TResult>>;
  formatOutput: (obj: TObj, results: BeefySnapshotCallResult<TResult>[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
  type MultipleResult<TResult> = { obj: TObj; result: TResult };
  type MaybeResult = { result: { blockNumber: number } | null };

  const fetchMulticallBlockDatetimeIfPossible = async (client: CustomViemClient, blockNumber: bigint): Promise<Date | null> => {
    if (!mcMap) {
      return null;
    }
    const multicallAddress = mcMap.multicallAddress as Hex;
    let blockDatetimePromise: Promise<Date | null> = Promise.resolve(null);
    if (options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
      blockDatetimePromise = client
        .readContract({
          abi: Multicall3Abi,
          address: multicallAddress,
          functionName: "getCurrentBlockTimestamp",
        })
        .then((res) => new Date(Number(res)));
    }
    return blockDatetimePromise;
  };

  const fetchJsonRpcBatch$: Rx.OperatorFunction<
    { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number> },
    {
      obj: TObj;
      query: JsonRpcBatchOutput<TQueryContent, number>;
      results: TResult[];
    }
  > = Rx.pipe(
    batchRpcCallsViem$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      logInfos: mergeLogsInfos({ msg: "maybe using multicall" }, options.logInfos),
      rpcCallsPerInputObj: options.rpcCallsPerInputObj,
      getQuery: ({ query }) => ({
        blockTag: getBlockTag(options.ctx, rangeToBlockNumber(query.range)),
        param: options.getCallParams(query.obj, rangeToBlockNumber(query.range)),
      }),
      processBatch: async (client, params) => {
        // also fetch datetime
        const datetimePromises = params.map(
          async (param) => [param, { blockDatetime: await fetchMulticallBlockDatetimeIfPossible(client, param.blockTag) }] as const,
        );
        const results = await options.processBatch(
          client,
          params.map(({ param }) => param),
        );
        const datetimes = await Promise.all(datetimePromises);
        const mergedResult = mergeBatchCallResults(results, { successes: new Map(datetimes), errors: new Map() });

        return mergedResult;
      },
      formatOutput: (item, result) => ({ ...item, results: [result] }),
    }),
  );

  const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;

  type ResultWithMabeBlockDatetime = { res: TRes; blockNumber: number; blockDatetime: Date | null };
  const fetchAddressBatch$: Rx.OperatorFunction<
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number> },
    { obj: TObj; query: AddressBatchOutput<TQueryContent, number>; results: MultipleResult<TResult>[] }
  > = Rx.pipe(
    Rx.tap((item) => logger.trace({ msg: "item", data: item })),

    Rx.mergeMap(async (item) => {
      if (!mcMap) {
        throw new ProgrammerError({
          msg: "Cannot use this method, multicall contract is not defined for this chain",
          data: { chain: options.ctx.chain },
        });
      }

      const multicallAddress = mcMap.multicallAddress as Hex;
      const blockTag = getBlockTag(options.ctx, rangeToBlockNumber(item.query.range));
      const client = options.ctx.rpcConfig.getViemClient("batch", options.logInfos, options.ctx.streamConfig);

      try {
        let blockDatetime: Date | null = null;

        let blockTimestampPromise: Promise<Date | null> = Promise.resolve(null);
        if (options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
          blockTimestampPromise = client
            .readContract({
              abi: Multicall3Abi,
              address: multicallAddress,
              functionName: "getCurrentBlockTimestamp",
            })
            .then((res) => new Date(Number(res)));
        }

        const res = await options.processBatch(client, item.query);

        return [{ ...item, res, blockNumber: blockTag, blockDatetime }];
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
    Rx.map((obj: TObj) => ({ obj, query: options.getQueryCallParams(obj) })),

    // handle different types of requests differently
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; query: JsonRpcBatchOutput<TQueryContent, number> } => isJsonRpcBatchQueries(item.query)),
          fetchJsonRpcBatch$,
        ),
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; query: AddressBatchOutput<TQueryContent, number> } => isAddressBatchQueries(item.query)),
          fetchAddressBatch$,
        ),
      ),
    ),
    Rx.map(({ obj, results }) => options.formatOutput(obj, results)),
  );
}

const getBlockTag = (ctx: ImportCtx, blockNumber: number): bigint => {
  // read the next block for those chains who can't read their own writes
  let blockTag = blockNumber;
  if (!ctx.rpcConfig.rpcLimitations.stateChangeReadsOnSameBlock) {
    blockTag = blockNumber + 1;
  }
  return BigInt(blockTag);
};

function rangeToBlockNumber(range: Range<number>): number {
  const midPoint = Math.round((range.from + range.to) / 2);
  if (!isInRange(range, midPoint)) {
    throw new ProgrammerError({ msg: "Midpoint is not in range, most likely an invalid range", data: { range, midPoint } });
  }
  return midPoint;
}
