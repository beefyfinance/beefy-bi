import * as Rx from "rxjs";
import { ethers } from "ethers";
import { BatchIntakeConfig, batchRpcCalls$ } from "../utils/batch-rpc-calls";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";
import { DbProduct } from "../loader/product";

export function fetchBlockDatetime$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TParams extends number,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  intakeConfig: BatchIntakeConfig;
  provider: ethers.providers.JsonRpcProvider;
  getBlockNumber: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter;
  formatOutput: (obj: TObj, blockDate: Date) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchRpcCalls$({
    intakeConfig: options.intakeConfig,
    processBatchKey: (obj: TObj) => {
      return options.getBlockNumber(obj) + "";
    },
    getQueryForBatch: (obj: TObj[]) => options.getBlockNumber(obj[0]),
    // do the actual processing
    processBatch: async (params: TParams[]) => {
      const promises: Promise<ethers.providers.Block>[] = [];

      for (const param of params) {
        const prom = options.provider.getBlock(param);
        promises.push(prom);
      }

      const blocks = await Promise.all(promises);
      return blocks.map((block) => new Date(block.timestamp * 1000));
    },
    logInfos: { msg: "mapping block datetimes" },
    emitErrors: options.emitErrors,
    formatOutput: options.formatOutput,
  });
}
