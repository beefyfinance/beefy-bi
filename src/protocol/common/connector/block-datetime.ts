import { ethers } from "ethers";
import { keyBy, uniqBy, zipWith } from "lodash";

export async function mapBlockDatetime<TObj, TKey extends string, TParams extends number>(
  provider: ethers.providers.JsonRpcProvider,
  objs: TObj[],
  getBlockNumber: (obj: TObj) => TParams,
  toKey: TKey,
): Promise<(TObj & { [key in TKey]: Date })[]> {
  // short circuit if there's nothing to do
  if (objs.length === 0) {
    return [];
  }
  const getKey = (blockNumber: TParams) => `${blockNumber}`;
  const blockNumbers = objs.map(getBlockNumber);
  const callsToMake = uniqBy(blockNumbers, getKey);

  // fetch all balances in one call
  const promises: Promise<ethers.providers.Block>[] = [];
  for (const param of callsToMake) {
    const prom = provider.getBlock(param);
    promises.push(prom);
  }

  const blockRes = await Promise.all(promises);

  const blockMap = keyBy(
    zipWith(callsToMake, blockRes, (param, block) => ({ param, block: block })),
    (res) => getKey(res.param),
  );

  const result = zipWith(
    objs,
    blockNumbers,
    (obj, param) =>
      ({
        ...obj,
        [toKey]: new Date(blockMap[getKey(param)].block.timestamp * 1000),
      } as TObj & { [key in TKey]: Date }),
  );
  return result;
}
