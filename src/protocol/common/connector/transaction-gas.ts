import { Decimal } from "decimal.js";
import { get, set } from "lodash";
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

  l1Fee?: Decimal;
  l1FeeScalar?: Decimal;
  l1GasPrice?: Decimal;
  l1GasUsed?: Decimal;
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
      const transactionHashes = params.map((p) => p.transactionHash);

      // it is probable we get multiple call for the same transaction hash
      const resultByHashPromises = transactionHashes.map(async (transactionHash) => {
        const receipt = await provider.getTransactionReceipt(transactionHash);
        const chain = options.ctx.rpcConfig.chain;
        let gasStats: TransactionGas = {
          chain,
          transactionHash: transactionHash,
          cumulativeGasUsed: new Decimal(receipt.cumulativeGasUsed.toString()),
          effectiveGasPrice: new Decimal(receipt.effectiveGasPrice?.toString()), // not provided by all chains
          gasUsed: new Decimal(receipt.gasUsed.toString()),
        };

        if (chain === "optimism") {
          gasStats.l1Fee = new Decimal(get(receipt, "l1Fee")?.toString() || "0");
          gasStats.l1FeeScalar = new Decimal(get(receipt, "l1FeeScalar")?.toString() || "0");
          gasStats.l1GasPrice = new Decimal(get(receipt, "l1GasPrice")?.toString() || "0");
          gasStats.l1GasUsed = new Decimal(get(receipt, "l1GasUsed")?.toString() || "0");
        }

        return [transactionHash, gasStats] as const;
      });
      const resultByHash = new Map(await Promise.all(resultByHashPromises));

      return new Map(params.map((param) => [param, resultByHash.get(param.transactionHash)!]));
    },
    formatOutput: options.formatOutput,
  });
}
