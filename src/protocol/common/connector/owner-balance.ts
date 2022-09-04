import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

export function mapERC20TokenBalance<
  TObj,
  TKey extends string,
  TParams extends { contractAddress: string; decimals: number; ownerAddress: string; blockNumber: number },
>(
  chain: Chain,
  provider: ethers.providers.JsonRpcProvider,
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: Decimal })[]> {
  const getKey = (param: TObj) => {
    const { contractAddress, ownerAddress, blockNumber } = getParams(param);
    return `${contractAddress}-${ownerAddress}-${blockNumber}`;
  };
  const process = async (params: TParams[]) => {
    const balancePromises: Promise<Decimal>[] = [];
    for (const param of params) {
      const valueMultiplier = new Decimal(10).pow(-param.decimals);
      const contract = new ethers.Contract(param.contractAddress, ERC20Abi, provider);

      // aurora RPC return the state before the transaction is applied
      let blockTag = param.blockNumber;
      if (chain === "aurora") {
        blockTag = param.blockNumber + 1;
      }

      const balancePromise = contract
        .balanceOf(param.ownerAddress, { blockTag })
        .then((balance: ethers.BigNumber) => valueMultiplier.mul(balance.toString() ?? "0"));
      balancePromises.push(balancePromise);
    }
    return Promise.all(balancePromises);
  };

  return batchQueryGroup(getParams, getKey, process, toKey);
}
