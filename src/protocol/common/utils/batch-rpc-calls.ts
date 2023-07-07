import { ethers } from "ethers";
import * as Rx from "rxjs";
import { Prettify } from "viem/dist/types/types/utils";
import { RpcCallMethod } from "../../../types/rpc-config";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { RpcLimitations } from "../../../utils/rpc/rpc-limitations";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../types/import-context";
import { cloneBatchProvider } from "./rpc-config";
import { CustomViemClient } from "./viem/client";

const logger = rootLogger.child({ module: "utils", component: "batch-rpc-calls" });

export type RPCBatchCallResult<TQueryObj, TQueryResp> = { successes: Map<TQueryObj, TQueryResp>; errors: Map<TQueryObj, ErrorReport> };

export function batchRpcCalls$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TQueryObj, TQueryResp>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getQuery: (obj: TObj) => TQueryObj;
  processBatch: (
    provider: ethers.providers.JsonRpcProvider | ethers.providers.JsonRpcBatchProvider,
    queryObjs: TQueryObj[],
  ) => Promise<RPCBatchCallResult<TQueryObj, TQueryResp>>;
  // we are doing this much rpc calls per input object
  // this is used to calculate the input batch to send to the client
  // and to know if we can inject the batch provider or if we should use the regular provider
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  logInfos: LogInfos;
  formatOutput: (objs: TObj, results: TQueryResp) => TRes;
}) {
  const { maxInputObjsPerBatch, canUseBatchProvider } = getBatchConfigFromLimitations({
    maxInputObjsPerBatch: options.ctx.streamConfig.maxInputTake,
    rpcCallsPerInputObj: options.rpcCallsPerInputObj,
    limitations: options.ctx.rpcConfig.rpcLimitations,
    logInfos: options.logInfos,
  });

  const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;
  return Rx.pipe(
    // add object TS type
    Rx.tap((_: TObj) => {}),

    // take a batch of items
    Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, maxInputObjsPerBatch),
    Rx.filter((objs) => objs.length > 0),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs) => {
      logger.trace(
        mergeLogsInfos(
          {
            msg: "batchRpcCalls$ - batch",
            data: {
              objsCount: objs.length,
              maxInputObjsPerBatch,
              canUseBatchProvider,
              workConcurrency,
              maxInputWaitMs: options.ctx.streamConfig.maxInputWaitMs,
            },
          },
          options.logInfos,
        ),
      );

      const objAndCallParams = objs.map((obj) => ({ obj, query: options.getQuery(obj) }));

      const work = async () => {
        logger.trace(mergeLogsInfos({ msg: "Ready to call RPC", data: { chain: options.ctx.chain } }, options.logInfos));

        const contractCalls = objAndCallParams.map(({ query }) => query);
        const res = await options.processBatch(provider, contractCalls);
        return res;
      };

      let provider: ethers.providers.JsonRpcProvider;
      if (canUseBatchProvider) {
        // create a clone of the provider so we can batch without locks
        // this is necessary because ethers relies on the node event loop to batch requests
        // and we are using a single provider for all calls
        // if we don't clone the provider, we will have a to make lock on the provider to make sure our batch object is
        // not used by another call somewhere else
        if (options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit") {
          provider = cloneBatchProvider(options.ctx.chain, options.ctx.behaviour, options.ctx.rpcConfig.batchProvider);
        } else {
          provider = options.ctx.rpcConfig.batchProvider;
        }
      } else {
        provider = options.ctx.rpcConfig.linearProvider;
      }

      try {
        const resultMap = await callLockProtectedRpc(work, {
          chain: options.ctx.chain,
          provider,
          rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
          logInfos: options.logInfos,
          maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
          noLockIfNoLimit: canUseBatchProvider, // no lock when using batch provider because we are using a copy of the provider
        });

        const outputs: TRes[] = [];
        for (const { obj, query } of objAndCallParams) {
          const errorReport = resultMap.errors.get(query);
          if (errorReport) {
            options.emitError(obj, errorReport);
            continue;
          }

          if (!resultMap.successes.has(query)) {
            logger.error(mergeLogsInfos({ msg: "result not found", data: { chain: options.ctx.chain, obj, query, resultMap } }, options.logInfos));
            throw new ProgrammerError(
              mergeLogsInfos({ msg: "result not found", data: { chain: options.ctx.chain, obj, query, resultMap } }, options.logInfos),
            );
          }
          const res = resultMap.successes.get(query) as TQueryResp;
          outputs.push(options.formatOutput(obj, res));
        }
        return outputs;
      } catch (error: any) {
        // here, none of the retrying worked, so we emit all the objects as in error
        const report: ErrorReport = {
          error,
          infos: mergeLogsInfos({ msg: "Error doing batch rpc work", data: { chain: options.ctx.chain, err: error } }, options.logInfos),
        };
        logger.debug(report.infos);
        logger.debug(report.error);
        for (const obj of objs) {
          report.infos.data = { ...report.infos.data, obj };
          options.emitError(obj, report);
        }
        return Rx.EMPTY;
      }
    }, workConcurrency),

    Rx.tap(
      (objs) =>
        Array.isArray(objs) &&
        logger.trace(mergeLogsInfos({ msg: "batchRpcCalls$ - done", data: { chain: options.ctx.chain, objsCount: objs.length } }, options.logInfos)),
    ),

    // flatten
    Rx.mergeAll(),
  );
}

export function batchRpcCallsViem$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TQueryObj, TQueryResp>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getQuery: (obj: TObj) => TQueryObj;
  processBatch: (provider: CustomViemClient, queryObjs: TQueryObj[]) => Promise<RPCBatchCallResult<TQueryObj, TQueryResp>>;
  // we are doing this much rpc calls per input object
  // this is used to calculate the input batch to send to the client
  // and to know if we can inject the batch provider or if we should use the regular provider
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  logInfos: LogInfos;
  formatOutput: (objs: TObj, results: TQueryResp) => TRes;
}) {
  const { maxInputObjsPerBatch, canUseBatchProvider } = getBatchConfigFromLimitations({
    maxInputObjsPerBatch: options.ctx.streamConfig.maxInputTake,
    rpcCallsPerInputObj: options.rpcCallsPerInputObj,
    limitations: options.ctx.rpcConfig.rpcLimitations,
    logInfos: options.logInfos,
  });

  const workConcurrency = options.ctx.rpcConfig.rpcLimitations.minDelayBetweenCalls === "no-limit" ? options.ctx.streamConfig.workConcurrency : 1;
  return Rx.pipe(
    // add object TS type
    Rx.tap((_: TObj) => {}),

    // take a batch of items
    Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, maxInputObjsPerBatch),
    Rx.filter((objs) => objs.length > 0),

    // for each batch, fetch the transfers
    Rx.mergeMap(async (objs) => {
      logger.trace(
        mergeLogsInfos(
          {
            msg: "batchRpcCalls$ - batch",
            data: {
              objsCount: objs.length,
              maxInputObjsPerBatch,
              canUseBatchProvider,
              workConcurrency,
              maxInputWaitMs: options.ctx.streamConfig.maxInputWaitMs,
            },
          },
          options.logInfos,
        ),
      );

      const objAndCallParams = objs.map((obj) => ({ obj, query: options.getQuery(obj) }));

      const rpcClient = options.ctx.rpcConfig.getViemClient(canUseBatchProvider ? "batch" : "linear", options.logInfos, options.ctx.streamConfig);

      try {
        logger.trace(mergeLogsInfos({ msg: "Ready to call RPC", data: { chain: options.ctx.chain } }, options.logInfos));

        const contractCalls = objAndCallParams.map(({ query }) => query);
        const resultMap = await options.processBatch(rpcClient, contractCalls);

        const outputs: TRes[] = [];
        for (const { obj, query } of objAndCallParams) {
          const errorReport = resultMap.errors.get(query);
          if (errorReport) {
            options.emitError(obj, errorReport);
            continue;
          }

          if (!resultMap.successes.has(query)) {
            logger.error(mergeLogsInfos({ msg: "result not found", data: { chain: options.ctx.chain, obj, query, resultMap } }, options.logInfos));
            throw new ProgrammerError(
              mergeLogsInfos({ msg: "result not found", data: { chain: options.ctx.chain, obj, query, resultMap } }, options.logInfos),
            );
          }
          const res = resultMap.successes.get(query) as TQueryResp;
          outputs.push(options.formatOutput(obj, res));
        }
        return outputs;
      } catch (error: any) {
        // here, none of the retrying worked, so we emit all the objects as in error
        const report: ErrorReport = {
          error,
          infos: mergeLogsInfos({ msg: "Error doing batch rpc work", data: { chain: options.ctx.chain, err: error } }, options.logInfos),
        };
        logger.debug(report.infos);
        logger.debug(report.error);
        for (const obj of objs) {
          report.infos.data = { ...report.infos.data, obj };
          options.emitError(obj, report);
        }
        return Rx.EMPTY;
      }
    }, workConcurrency),

    Rx.tap(
      (objs) =>
        Array.isArray(objs) &&
        logger.trace(mergeLogsInfos({ msg: "batchRpcCalls$ - done", data: { chain: options.ctx.chain, objsCount: objs.length } }, options.logInfos)),
    ),

    // flatten
    Rx.mergeAll(),
  );
}

export function getBatchConfigFromLimitations(options: {
  maxInputObjsPerBatch: number;
  rpcCallsPerInputObj: {
    [method in RpcCallMethod]: number;
  };
  limitations: RpcLimitations;
  logInfos: LogInfos;
}) {
  // get the rpc provider maximum batch size
  const methodLimitations = options.limitations.methods;

  // find out the max number of objects to process in a batch
  let canUseBatchProvider = true;
  let maxInputObjsPerBatch = options.maxInputObjsPerBatch;
  for (const [method, count] of Object.entries(options.rpcCallsPerInputObj)) {
    // we don't use this method so we don't care
    if (count <= 0) {
      continue;
    }
    const maxCount = methodLimitations[method as RpcCallMethod];
    if (maxCount === null) {
      canUseBatchProvider = false;
      logger.trace(mergeLogsInfos({ msg: "disabling batch provider since limitation is null", data: { method } }, options.logInfos));
      break;
    }
    const newMax = Math.max(1, Math.min(maxInputObjsPerBatch, Math.floor(maxCount / count)));
    logger.trace(
      mergeLogsInfos(
        { msg: "updating maxInputObjsPerBatch with provided count per item", data: { old: maxInputObjsPerBatch, new: newMax } },
        options.logInfos,
      ),
    );
    maxInputObjsPerBatch = newMax;
  }

  if (!canUseBatchProvider) {
    logger.trace(
      mergeLogsInfos({ msg: "setting maxInputObjsPerBatch to 1 since we disabled batching", data: { maxInputObjsPerBatch } }, options.logInfos),
    );
    maxInputObjsPerBatch = 1;
  } else if (maxInputObjsPerBatch <= 1) {
    canUseBatchProvider = false;
  }
  logger.debug(mergeLogsInfos({ msg: "config", data: { maxInputObjsPerBatch, canUseBatchProvider, methodLimitations } }, options.logInfos));

  return { maxInputObjsPerBatch, canUseBatchProvider };
}

export function mergeBatchCallResults<TQueryObj, TQueryRespA, TQueryRespB>(
  resultA: RPCBatchCallResult<TQueryObj, TQueryRespA>,
  resultB: RPCBatchCallResult<TQueryObj, TQueryRespB>,
): RPCBatchCallResult<TQueryObj, Prettify<TQueryRespA & TQueryRespB>> {
  const successes = new Map<TQueryObj, TQueryRespA & TQueryRespB>();
  const errors = new Map<TQueryObj, ErrorReport>();
  const allKeys = Array.from(resultA.successes.keys()).concat(Array.from(resultB.successes.keys()));
  for (const key of allKeys) {
    const resA = resultA.successes.get(key);
    const resB = resultB.successes.get(key);
    const errA = resultA.errors.get(key);
    const errB = resultB.errors.get(key);

    if (errA || errB) {
      errors.set(key, errA || errB!);
    } else if (!resA || !resB) {
      throw new ProgrammerError({ msg: "mergeBatchCallResults - missing result", data: { key, resA, resB, errA, errB } });
    } else {
      successes.set(key, { ...resA, ...resB });
    }
  }
  return { successes, errors };
}
