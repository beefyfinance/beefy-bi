import { Decimal } from "decimal.js";
import { ethers } from "ethers";
import * as Rx from "rxjs";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { ImportCtx } from "../types/import-context";
import { batchRpcCalls$ } from "../utils/batch-rpc-calls";

interface GetBalanceCallParams {
  contractAddress: string;
  decimals: number;
  ownerAddress: string;
  blockNumber: number;
}

export function fetchERC20TokenBalance$<TObj, TCtx extends ImportCtx<TObj>, TRes, TParams extends GetBalanceCallParams>(options: {
  ctx: TCtx;
  getQueryParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, balance: Decimal) => TRes;
}) {
  return batchRpcCalls$({
    ctx: options.ctx,
    rpcCallsPerInputObj: {
      eth_call: 1,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
    },
    logInfos: { msg: "Fetching ERC20 token balance", data: {} },
    formatOutput: options.formatOutput,
    getQuery: options.getQueryParams,
    processBatch: async (provider, params: TParams[]) => {
      const balancePromises: Promise<Decimal>[] = [];
      for (const param of params) {
        const valueMultiplier = new Decimal(10).pow(-param.decimals);
        const contract = new ethers.Contract(param.contractAddress, ERC20Abi, provider);

        // aurora RPC return the state before the transaction is applied
        let blockTag = param.blockNumber;
        if (options.ctx.rpcConfig.chain === "aurora") {
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
