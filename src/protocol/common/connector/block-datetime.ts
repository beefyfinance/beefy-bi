import * as Rx from "rxjs";
import { ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

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
      const promises = params.map((blockNumber) => provider.getBlock(blockNumber));
      const blocks = await Promise.all(promises);
      return blocks.map((block) => new Date(block.timestamp * 1000));
    },
    formatOutput: options.formatOutput,
    logInfos: { msg: "Fetching block datetime", data: {} },
  });
}
