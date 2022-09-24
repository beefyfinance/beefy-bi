import * as Rx from "rxjs";
import { ethers } from "ethers";
import { batchQueryGroup$ } from "../../../utils/rxjs/utils/batch-query-group";
import { retryRpcErrors } from "../../../utils/rxjs/utils/retry-rpc";

export function fetchBlockDatetime$<TObj, TParams extends number, TRes>(options: {
  provider: ethers.providers.JsonRpcProvider;
  getBlockNumber: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockDate: Date) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    batchQueryGroup$({
      bufferCount: 500,
      toQueryObj: (obj: TObj[]) => options.getBlockNumber(obj[0]),
      getBatchKey: (obj: TObj) => {
        return options.getBlockNumber(obj) + "";
      },
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
      formatOutput: options.formatOutput,
    }),

    // we want to catch any errors from the RPC
    retryRpcErrors({ msg: "mapping block datetimes" }),
  );
}
