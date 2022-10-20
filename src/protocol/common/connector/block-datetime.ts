import { keyBy, uniq, zipWith } from "lodash";
import { rootLogger } from "../../../utils/logger";
import { ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "common", component: "block-datetime" });

export function fetchBlockDatetime$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends number>(options: {
  ctx: TCtx;
  getBlockNumber: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: Date) => TRes;
}) {
  return batchRpcCalls$({
    ctx: options.ctx,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 1,
      eth_getLogs: 0,
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
    formatOutput: options.formatOutput,
    logInfos: { msg: "Fetching block datetime", data: {} },
  });
}
