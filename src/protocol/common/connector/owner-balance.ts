import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { ErrorEmitter, ImportQuery } from "../types/import-query";
import { batchRpcCalls$, BatchStreamConfig } from "../utils/batch-rpc-calls";

interface GetBalanceCallParams {
  contractAddress: string;
  decimals: number;
  ownerAddress: string;
  blockNumber: number;
}

export function fetchERC20TokenBalance$<
  TTarget,
  TObj extends ImportQuery<TTarget, number>,
  TParams extends GetBalanceCallParams,
  TRes extends ImportQuery<TTarget, number>,
>(options: {
  rpcConfig: RpcConfig;
  chain: Chain;
  getQueryParams: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter<TTarget, number>;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, balance: Decimal) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchRpcCalls$({
    rpcConfig: options.rpcConfig,
    streamConfig: options.streamConfig,
    rpcCallsPerInputObj: {
      eth_call: 1,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
    },
    logInfos: { msg: "Fetching ERC20 token balance", data: {} },
    emitErrors: options.emitErrors,
    formatOutput: options.formatOutput,
    getQuery: options.getQueryParams,
    processBatch: async (provider, params: TParams[]) => {
      const balancePromises: Promise<Decimal>[] = [];
      for (const param of params) {
        const valueMultiplier = new Decimal(10).pow(-param.decimals);
        const contract = new ethers.Contract(param.contractAddress, ERC20Abi, provider);

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
  });
}
