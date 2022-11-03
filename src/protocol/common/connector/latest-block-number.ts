import * as Rx from "rxjs";
import { db_query_one } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { cacheOperatorResult$ } from "../../../utils/rxjs/utils/cache-operator-result";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "latest-block-number" });

export function latestBlockNumber$<TObj, TCtx extends ImportCtx<TObj>, TRes>(options: {
  ctx: TCtx;
  forceCurrentBlockNumber: number | null;
  formatOutput: (obj: TObj, latestBlockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return cacheOperatorResult$({
    stdTTLSec: 60 /* 1min */,
    getCacheKey: () => options.ctx.chain,
    logInfos: { msg: "latest block number", data: { chain: options.ctx.chain } },
    operator$: Rx.mergeMap(async (obj) => {
      if (options.forceCurrentBlockNumber) {
        logger.info({ msg: "Using forced block number", data: { blockNumber: options.forceCurrentBlockNumber, chain: options.ctx.chain } });
        return { input: obj, output: options.forceCurrentBlockNumber };
      }

      try {
        const latestBlockNumber = await callLockProtectedRpc(() => options.ctx.rpcConfig.linearProvider.getBlockNumber(), {
          chain: options.ctx.chain,
          provider: options.ctx.rpcConfig.linearProvider,
          rpcLimitations: options.ctx.rpcConfig.limitations,
          logInfos: { msg: "latest block number", data: { chain: options.ctx.rpcConfig.chain } },
          maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
        });
        return { input: obj, output: latestBlockNumber };
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
        [options.ctx.rpcConfig.chain],
        options.ctx.client,
      );
      if (!dbRes) {
        throw new Error(`No block number found for chain ${options.ctx.rpcConfig.chain}`);
      }
      return { input: obj, output: dbRes.latest_block_number };
    }, options.ctx.streamConfig.workConcurrency),
    formatOutput: options.formatOutput,
  });
}
