import { Decimal } from "decimal.js";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "common", component: "transaction-gas" });

export interface TransactionGas {
  chain: Chain;

  transactionHash: string;

  effectiveGasPrice: Decimal;
  cumulativeGasUsed: Decimal;
  gasUsed: Decimal;
}

interface GetTransactionGasCallParams {
  transactionHash: string;
}

export function fetchTransactionGas$<TObj, TCtx extends ImportCtx<TObj>, TRes>(options: {
  ctx: TCtx;
  getQueryParams: (obj: TObj) => GetTransactionGasCallParams;
  formatOutput: (obj: TObj, gas: TransactionGas) => TRes;
}) {
  return batchRpcCalls$({
    ctx: options.ctx,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
      eth_getTransactionReceipt: 1,
    },
    logInfos: { msg: "Fetching ERC20 transfers", data: { chain: options.ctx.rpcConfig.chain } },
    getQuery: options.getQueryParams,
    processBatch: async (provider, params: GetTransactionGasCallParams[]) => {
      const promises = params.map(async (param) => {
        const receipt = await provider.getTransactionReceipt(param.transactionHash);
        const gasStats: TransactionGas = {
          chain: options.ctx.rpcConfig.chain,
          transactionHash: param.transactionHash,
          cumulativeGasUsed: new Decimal(receipt.cumulativeGasUsed.toString()),
          effectiveGasPrice: new Decimal(receipt.effectiveGasPrice.toString()),
          gasUsed: new Decimal(receipt.gasUsed.toString()),
        };
        return [param, gasStats] as const;
      });
      return new Map(await Promise.all(promises));
    },
    formatOutput: options.formatOutput,
  });
}
