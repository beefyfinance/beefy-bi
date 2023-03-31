import { Block } from "@ethersproject/abstract-provider";
import { uniq } from "lodash";
import NodeCache from "node-cache";
import * as Rx from "rxjs";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlock$, upsertBlock$ } from "../loader/blocks";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "common", component: "block-datetime" });

export function fetchBlockDatetime$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends number>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getBlockNumber: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: Date) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const chain = options.ctx.chain;

  // avoid re-fetching the same block datetime again and again
  // if many requests arrive with the same blockNumber at once
  const cache = new NodeCache({
    stdTTL: 5,
    useClones: false,
  });

  const fetchFromRPC$ = Rx.pipe(
    // add TS typings
    Rx.tap((_: TObj) => {}),

    // fetch block from our RPC
    batchRpcCalls$({
      emitError: options.emitError,
      ctx: options.ctx,
      rpcCallsPerInputObj: {
        eth_call: 0,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 1,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: options.getBlockNumber,
      processBatch: async (provider, params: TParams[]) => {
        const uniqBlockNumbers = uniq(params);

        const entries = await Promise.all(
          uniqBlockNumbers.map(async (blockNumber) => {
            // cache the promise directly so that we don't have to wait for the call to end to cache the value
            // this increases the cache hit rate
            const cachedPromise = cache.get<Promise<[TParams, Date]>>(blockNumber);
            if (cachedPromise) {
              return cachedPromise;
            } else {
              const prom = (async () => {
                const block: Block = await provider.getBlock(blockNumber);
                const date = new Date(block.timestamp * 1000);
                return [blockNumber, date] as const;
              })();
              cache.set(blockNumber, prom);
              return prom;
            }
          }),
        );
        return { successes: new Map(entries), errors: new Map() };
      },
      formatOutput: (obj, blockDate) => ({ obj, blockDate }),
      logInfos: { msg: "Fetching block datetime", data: {} },
    }),

    // save block in the database so we can fetch it later
    upsertBlock$({
      ctx: {
        ...options.ctx,
        // make sure we are aligned with the RPC config so we have an overall consistent behaviour
        streamConfig: {
          ...options.ctx.streamConfig,
          dbMaxInputTake: options.ctx.streamConfig.maxInputTake,
          dbMaxInputWaitMs: options.ctx.streamConfig.maxInputWaitMs,
        },
      },
      emitError: (obj, report) => {
        logger.error(mergeLogsInfos({ msg: "Failed to upsert block", data: { chain, obj } }, report.infos));
        logger.error(report.error);
        throw new ProgrammerError("Failed to upsert block");
      },
      getBlockData: (item) => ({
        datetime: item.blockDate,
        chain,
        blockNumber: options.getBlockNumber(item.obj),
        blockData: {},
      }),
      formatOutput: (item) => options.formatOutput(item.obj, item.blockDate),
    }),
  );

  return Rx.pipe(
    // fetch our block from the database
    fetchBlock$({
      ctx: {
        ...options.ctx,
        streamConfig: {
          ...options.ctx.streamConfig,
          // make sure we are aligned with the RPC config so we have an overall consistent behaviour
          dbMaxInputTake: options.ctx.streamConfig.maxInputTake,
          dbMaxInputWaitMs: options.ctx.streamConfig.maxInputWaitMs,
        },
      },
      emitError: options.emitError,
      chain: chain,
      getBlockNumber: options.getBlockNumber,
      formatOutput: (obj, dbBlock) => ({ obj, dbBlock }),
    }),

    // if we found the block, return it
    // if not, we fetch it from the RPC
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          excludeNullFields$("dbBlock"),
          Rx.tap((item) => logger.trace({ msg: "Found block in database", data: { chain, blockNumber: options.getBlockNumber(item.obj) } })),
          Rx.map((item) => options.formatOutput(item.obj, item.dbBlock.datetime)),
        ),
        items$.pipe(
          Rx.filter((item) => item.dbBlock === null),
          Rx.tap((item) => logger.trace({ msg: "Block not found in database", data: { chain, blockNumber: options.getBlockNumber(item.obj) } })),
          Rx.map((item) => item.obj),
          fetchFromRPC$,
        ),
      ),
    ),
  );
}
