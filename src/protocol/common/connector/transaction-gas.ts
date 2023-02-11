import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import { get } from "lodash";
import { Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
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

export function fetchTransactionGas$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getQueryParams: (obj: TObj) => GetTransactionGasCallParams;
  formatOutput: (obj: TObj, gas: TransactionGas) => TRes;
}) {
  return batchRpcCalls$({
    ctx: options.ctx,
    emitError: options.emitError,
    rpcCallsPerInputObj: {
      eth_call: 0,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
      eth_getTransactionReceipt: 1,
    },
    logInfos: { msg: "Fetching Transaction gas", data: { chain: options.ctx.chain } },
    getQuery: options.getQueryParams,
    processBatch: async (provider, params: GetTransactionGasCallParams[]) => {
      const transactionHashes = params.map((p) => p.transactionHash);

      // it is probable we get multiple call for the same transaction hash
      const resultByHashPromises = transactionHashes.map(async (transactionHash) => {
        const receipt = await provider.getTransactionReceipt(transactionHash);
        // log the address if the receipt is not in the format we expect
        if (!receipt || !receipt.cumulativeGasUsed || !receipt.gasUsed) {
          logger.error({ msg: "Invalid transaction receipt", data: { chain: options.ctx.chain, transactionHash, receipt } });
        }

        const chain = options.ctx.chain;
        let gasStats: TransactionGas = {
          chain,
          transactionHash: transactionHash,
          cumulativeGasUsed: new Decimal(receipt.cumulativeGasUsed.toString()),
          gasUsed: new Decimal(receipt.gasUsed.toString()),
          effectiveGasPrice: new Decimal(receipt.effectiveGasPrice?.toString() || "0"), // this field is not in the spec so many RPC providers don't return it
        };

        if (chain === "optimism" || chain === "metis") {
          const l2Receipt = receipt as unknown as {
            l1Fee?: ethers.BigNumber;
            l1FeeScalar?: ethers.BigNumber;
            l1GasPrice?: ethers.BigNumber;
            l1GasUsed?: ethers.BigNumber;
          };
          gasStats.l1Fee = new Decimal(get(l2Receipt, "l1Fee")?.toString() || "0");
          gasStats.l1FeeScalar = new Decimal(get(l2Receipt, "l1FeeScalar")?.toString() || "0");
          gasStats.l1GasPrice = new Decimal(get(l2Receipt, "l1GasPrice")?.toString() || "0");
          gasStats.l1GasUsed = new Decimal(get(l2Receipt, "l1GasUsed")?.toString() || "0");
        }

        return [transactionHash, gasStats] as const;
      });
      const resultByHash = new Map(await Promise.all(resultByHashPromises));

      return new Map(params.map((param) => [param, resultByHash.get(param.transactionHash)!]));
    },
    formatOutput: options.formatOutput,
  });
}
