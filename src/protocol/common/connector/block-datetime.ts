import * as Rx from "rxjs";
import { ethers } from "ethers";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

export function mapBlockDatetime<TObj, TKey extends string, TParams extends number>(
  provider: ethers.providers.JsonRpcProvider,
  getBlockNumber: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: Date })[]> {
  const toQueryObj = (obj: TObj[]) => getBlockNumber(obj[0]);

  const process = async (params: TParams[]) => {
    const promises: Promise<ethers.providers.Block>[] = [];

    for (const param of params) {
      const prom = provider.getBlock(param);
      promises.push(prom);
    }

    const blocks = await Promise.all(promises);
    return blocks.map((block) => new Date(block.timestamp * 1000));
  };

  return batchQueryGroup(toQueryObj, getBlockNumber, process, toKey);
}
