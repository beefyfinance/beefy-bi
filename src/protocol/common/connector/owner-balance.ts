import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { Decimal } from "decimal.js";
import { keyBy, uniqBy, zipWith } from "lodash";

export async function mapERC20TokenBalance<
  TObj,
  TKey extends string,
  TParams extends { contractAddress: string; decimals: number; ownerAddress: string; blockNumber: number },
>(
  provider: ethers.providers.JsonRpcProvider,
  objs: TObj[],
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Promise<(TObj & { [key in TKey]: Decimal })[]> {
  // short circuit if there's nothing to do
  if (objs.length === 0) {
    return [];
  }
  const getKey = (param: TParams) => `${param.contractAddress}-${param.ownerAddress}-${param.blockNumber}`;
  const params = objs.map(getParams);
  const callsToMake = uniqBy(params, getKey);

  // fetch all balances in one call
  const balancePromises: Promise<Decimal>[] = [];
  for (const param of callsToMake) {
    const valueMultiplier = new Decimal(10).pow(-param.decimals);
    const contract = new ethers.Contract(param.contractAddress, ERC20Abi, provider);
    const balancePromise = contract
      .balanceOf(param.ownerAddress, { blockTag: param.blockNumber })
      .then((balance: ethers.BigNumber) => valueMultiplier.mul(balance.toString() ?? "0"));
    balancePromises.push(balancePromise);
  }

  const balancesRes = await Promise.all(balancePromises);
  const balanceMap = keyBy(
    zipWith(callsToMake, balancesRes, (param, balance) => ({ param, balance })),
    (res) => getKey(res.param),
  );

  const result = zipWith(
    objs,
    params,
    (obj, param) =>
      ({
        ...obj,
        [toKey]: balanceMap[getKey(param)].balance,
      } as TObj & { [key in TKey]: Decimal }),
  );
  return result;
}
