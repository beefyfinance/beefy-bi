import * as Rx from "rxjs";
import { RpcConfig } from "../../../types/rpc-config";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { batchRpcCalls$, BatchStreamConfig } from "../utils/batch-rpc-calls";

export function fetchBlockDatetime$<
  TTarget,
  TObj extends ImportQuery<TTarget, number>,
  TParams extends number,
  TRes extends ImportQuery<TTarget, number>,
>(options: {
  streamConfig: BatchStreamConfig;
  rpcConfig: RpcConfig;
  getBlockNumber: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter<TTarget, number>;
  formatOutput: (obj: TObj, blockDate: Date) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchRpcCalls$({
    rpcConfig: options.rpcConfig,
    streamConfig: options.streamConfig,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 1,
      eth_getLogs: 0,
    },
    getQuery: options.getBlockNumber,
    processBatch: async (provider, params: TParams[]) => {
      const promises = params.map((blockNumber) => provider.getBlock(blockNumber));
      return Promise.all(promises).then((blocks) => blocks.map((block) => new Date(block.timestamp * 1000)));
    },
    formatOutput: options.formatOutput,
    emitErrors: options.emitErrors,
    logInfos: { msg: "Fetching block datetime", data: {} },
  });
}
