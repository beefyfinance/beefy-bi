import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { batchQueryGroup$ } from "../../../utils/rxjs/utils/batch-query-group";
import { retryRpcErrors } from "../../../utils/rxjs/utils/retry-rpc";

export function fetchERC20TokenBalance$<
  TObj,
  TParams extends { contractAddress: string; decimals: number; ownerAddress: string; blockNumber: number },
  TRes,
>(options: {
  provider: ethers.providers.JsonRpcProvider;
  chain: Chain;
  getQueryParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, balance: Decimal) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    batchQueryGroup$({
      bufferCount: 200,
      // there should be only one query for each group since we batch by owner and contract address
      toQueryObj: (obj: TObj[]) => options.getQueryParams(obj[0]),
      // we process all transfers by individual user
      getBatchKey: (param: TObj) => {
        const { contractAddress, ownerAddress, blockNumber } = options.getQueryParams(param);
        return `${contractAddress}-${ownerAddress}-${blockNumber}`;
      },
      // do the actual processing
      processBatch: async (params: TParams[]) => {
        const balancePromises: Promise<Decimal>[] = [];
        for (const param of params) {
          const valueMultiplier = new Decimal(10).pow(-param.decimals);
          const contract = new ethers.Contract(param.contractAddress, ERC20Abi, options.provider);

          // aurora RPC return the state before the transaction is applied
          let blockTag = param.blockNumber;
          if (options.chain === "aurora") {
            blockTag = param.blockNumber + 1;
          }

          const balancePromise = contract
            .balanceOf(param.ownerAddress, { blockTag })
            .then((balance: ethers.BigNumber) => valueMultiplier.mul(balance.toString() ?? "0"));
          balancePromises.push(balancePromise);
        }
        return Promise.all(balancePromises);
      },
      formatOutput: options.formatOutput,
    }),

    retryRpcErrors({ msg: "mapping owner balance" }),
  );
}
