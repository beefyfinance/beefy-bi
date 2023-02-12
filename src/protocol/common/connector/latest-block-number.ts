import * as Rx from "rxjs";
import { db_query_one } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ErrorEmitter, ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "latest-block-number" });

export function latestBlockNumber$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  forceCurrentBlockNumber: number | null;
  formatOutput: (obj: TObj, latestBlockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // take a batch of items
    Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, options.ctx.streamConfig.maxInputTake),
    Rx.filter((objs) => objs.length > 0),

    cacheOperatorResult$({
      cacheConfig: {
        type: "global",
        globalKey: "latest-block-number",
        stdTTLSec: 10 /* 10 sec */,
        useClones: false,
      },
      getCacheKey: () => options.ctx.chain,
      logInfos: { msg: "latest block number", data: { chain: options.ctx.chain } },
      operator$: Rx.mergeMap(async (objs) => {
        if (options.forceCurrentBlockNumber) {
          logger.info({ msg: "Using forced block number", data: { blockNumber: options.forceCurrentBlockNumber, chain: options.ctx.chain } });
          return { input: objs, output: options.forceCurrentBlockNumber };
        }

        try {
          const latestBlockNumber = await callLockProtectedRpc(() => options.ctx.rpcConfig.linearProvider.getBlockNumber(), {
            chain: options.ctx.chain,
            provider: options.ctx.rpcConfig.linearProvider,
            rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
            logInfos: { msg: "latest block number", data: { chain: options.ctx.chain } },
            maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
            noLockIfNoLimit: true, // we use linearProvider, so this has no effect
          });
          logger.trace({ msg: "Latest block number from rpc query", data: { latestBlockNumber, chain: options.ctx.chain } });
          return { input: objs, output: latestBlockNumber };
        } catch (err) {
          logger.error({ msg: "Error while fetching latest block number", data: { chain: options.ctx.chain, err } });
        }
        logger.info({ msg: "Using last block number from database", data: { chain: options.ctx.chain } });

        const dbRes = await db_query_one<{ latest_block_number: number }>(
          `
          select last(block_number, datetime) as latest_block_number 
          from block_ts 
          where chain = %L
        `,
          [options.ctx.chain],
          options.ctx.client,
        );
        if (!dbRes) {
          throw new Error(`No block number found for chain ${options.ctx.chain}`);
        }
        logger.trace({ msg: "Latest block number from db", data: { latestBlockNumber: dbRes.latest_block_number, chain: options.ctx.chain } });
        return { input: objs, output: dbRes.latest_block_number };
      }, options.ctx.streamConfig.workConcurrency),
      formatOutput: (objs, latestBlockNumber: number) => ({ objs, latestBlockNumber }),
    }),

    // flatten objects
    Rx.concatMap(({ objs, latestBlockNumber }) => Rx.from(objs).pipe(Rx.map((obj) => options.formatOutput(obj, latestBlockNumber)))),
  );
}
