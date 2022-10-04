import * as Rx from "rxjs";
import { ethers } from "ethers";
import { BatchStreamConfig, batchRpcCalls$ } from "../utils/batch-rpc-calls";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { DbProduct } from "../loader/product";

export function fetchBlockDatetime$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TParams extends number,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  streamConfig: BatchStreamConfig;
  provider: ethers.providers.JsonRpcProvider;
  getBlockNumber: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter;
  formatOutput: (obj: TObj, blockDate: Date) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchRpcCalls$({
    streamConfig: options.streamConfig,
    getQuery: options.getBlockNumber,
    processBatch: async (params: TParams[]) => {
      const promises = params.map((blockNumber) => options.provider.getBlock(blockNumber));
      return Promise.all(promises).then((blocks) => blocks.map((block) => new Date(block.timestamp * 1000)));
    },
    formatOutput: options.formatOutput,
    emitErrors: options.emitErrors,
    logInfos: { msg: "Fetching block datetime", data: {} },
  });
}
