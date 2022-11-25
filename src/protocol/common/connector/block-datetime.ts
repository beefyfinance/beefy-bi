import { keyBy, uniq } from "lodash";
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
        const blocks = await Promise.all(uniqBlockNumbers.map((blockNumber) => provider.getBlock(blockNumber)));
        const blockByNumberMap = keyBy(blocks, "number");

        const result = new Map(
          params.map((blockNumber) => {
            const block = blockByNumberMap[blockNumber];
            if (block === undefined) {
              logger.error({ msg: "block date not found", data: { blockNumber, blockByNumberMap, params } });
              throw new Error(`Block ${blockNumber} not found`);
            }
            return [blockNumber, new Date(block.timestamp * 1000)];
          }),
        );
        return result;
      },
      formatOutput: (obj, blockDate) => ({ obj, blockDate }),
      logInfos: { msg: "Fetching block datetime", data: {} },
    }),

    // save block in the database so we can fetch it later
    upsertBlock$({
      ctx: {
        ...options.ctx,
        // make sure we are aligned with the RPC config so we have an overall consistent behavior
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
          // make sure we are aligned with the RPC config so we have an overall consistent behavior
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
