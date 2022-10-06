import * as Rx from "rxjs";
import { ethers } from "ethers";
import { BatchStreamConfig, batchRpcCalls$ } from "../utils/batch-rpc-calls";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { DbProduct } from "../loader/product";
import { RpcConfig } from "../../../types/rpc-config";

export function fetchBlockDatetime$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TParams extends number,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  streamConfig: BatchStreamConfig;
  rpcConfig: RpcConfig;
  getBlockNumber: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter;
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
