import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import { groupBy } from "lodash";
import { ERC20AbiInterface } from "../../../utils/abi";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

interface GetBalanceCallParams {
  contractAddress: string;
  decimals: number;
  ownerAddress: string;
  blockNumber: number;
}

export function fetchERC20TokenBalance$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends GetBalanceCallParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getQueryParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, balance: Decimal) => TRes;
}) {
  return batchRpcCalls$({
    ctx: options.ctx,
    emitError: options.emitError,
    rpcCallsPerInputObj: {
      eth_call: 1,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
      eth_getTransactionReceipt: 0,
    },
    logInfos: { msg: "Fetching ERC20 token balance", data: {} },
    formatOutput: options.formatOutput,
    getQuery: options.getQueryParams,
    processBatch: async (provider, params: TParams[]) => {
      // deduplicate calls to make so we don't ask for the same balance twice
      const paramsByCalls = groupBy(params, (p) => `${p.contractAddress}-${p.ownerAddress}-${p.blockNumber}`);
      const calls = Object.values(paramsByCalls).map(async (params) => {
        const param = params[0];
        const valueMultiplier = new Decimal(10).pow(-param.decimals);
        const contract = new ethers.Contract(param.contractAddress, ERC20AbiInterface, provider);

        // aurora RPC return the state before the transaction is applied
        // todo: patch ethers.js to reflect this behavior
        let blockTag = param.blockNumber;
        if (options.ctx.chain === "aurora") {
          blockTag = param.blockNumber + 1;
        }

        const rawBalance = await contract.balanceOf(param.ownerAddress, { blockTag });
        const balance = valueMultiplier.mul(rawBalance.toString() ?? "0");
        return params.map((p) => [p, balance] as const);
      });
      const results = await Promise.all(calls);
      return new Map(results.flat());
    },
  });
}
