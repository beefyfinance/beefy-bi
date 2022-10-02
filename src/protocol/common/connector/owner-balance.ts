import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ethers } from "ethers";
import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { BatchIntakeConfig, batchRpcCalls$ } from "../utils/batch-rpc-calls";
import { DbProduct } from "../loader/product";
import { ErrorEmitter, ProductImportQuery } from "../types/product-query";

interface GetBalanceCallParams {
  contractAddress: string;
  decimals: number;
  ownerAddress: string;
  blockNumber: number;
}

export function fetchERC20TokenBalance$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TParams extends GetBalanceCallParams,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  provider: ethers.providers.JsonRpcProvider;
  chain: Chain;
  getQueryParams: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter;
  intakeConfig: BatchIntakeConfig;
  formatOutput: (obj: TObj, balance: Decimal) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchRpcCalls$({
    intakeConfig: options.intakeConfig,
    // there should be only one query for each group since we batch by owner and contract address
    getQueryForBatch: (obj: TObj[]) => options.getQueryParams(obj[0]),
    // we process all transfers by individual user
    processBatchKey: (param: TObj) => {
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
    emitErrors: options.emitErrors,
    logInfos: { msg: "Fetching ERC20 token balances" },
    formatOutput: options.formatOutput,
  });
}
