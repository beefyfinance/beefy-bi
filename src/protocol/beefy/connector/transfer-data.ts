import Decimal from "decimal.js";
import { min, uniq } from "lodash";
import { Hex } from "../../../types/address";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { MulticallCallOneCallParamStructure } from "../../../utils/rpc/call";
import { RpcRequestBatch } from "../../../utils/rpc/jsonrpc-batch";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "beefy", component: "transfer-data" });

type BeefyTransferCallParams = {
  vaultAddress: Hex;
  vaultDecimals: number;
  investorAddress: Hex;
  underlyingDecimals: number;
  blockNumber: number;
};

/**
 * Fetch every single data we need to be associated with a transfer
 * @param options
 * @returns
 */
export function fetchBeefyTransferData$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends BeefyTransferCallParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPPFSCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, ppfs: Decimal) => TRes;
}) {
  const logInfos = { msg: "Fetching Beefy PPFS", data: { chain: options.ctx.chain } };

  return batchRpcCalls$({
    ctx: options.ctx,
    emitError: options.emitError,
    logInfos,
    rpcCallsPerInputObj: {
      eth_call: 3,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
      eth_getTransactionReceipt: 0,
    },
    getQuery: options.getPPFSCallParams,
    processBatch: (provider, params) => {
      const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
      // find out if we can use multicall
      const minBlockNumber = min(params.map((c) => c.blockNumber)) || 0;
      if (
        // this chain has the Multicall3 contract deployed
        mcMap &&
        // the block number we work on is after the mc contract creation
        mcMap.createdAtBlock < minBlockNumber
      ) {
        const multicallAddress = mcMap.multicallAddress;
        const jsonRpcBatch: RpcRequestBatch<[MulticallCallOneCallParamStructure[]], { success: boolean; returnData: Hex }[]> = [];
        for (const param of params) {
          // create the multicall function call
          const multicallData: MulticallCallOneCallParamStructure[] = [
            {
              allowFailure: true, // can fail when vault is empty
              callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
              target: param.vaultAddress,
            },
            {
              allowFailure: false,
              callData: BeefyVaultV6AbiInterface.encodeFunctionData("balanceOf", [param.investorAddress]),
              target: param.vaultAddress,
            },
            {
              allowFailure: false,
              callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
              target: multicallAddress,
            },
          ];

          // create the multicall eth_call call at the right block number
          jsonRpcBatch.push({
            interface: Multicall3AbiInterface,
            contractAddress: multicallAddress,
            params: [multicallData],
            function: "aggregate3",
            blockTag: transfer.blockNumber,
          });
        }

        const result = await jsonRpcBatchEthCall({ url: "https://rpc.ankr.com/bsc", requests: jsonRpcBatch });

        for (const [_, res] of result.entries()) {
          const itemData = await res;
          console.dir({ itemData }, { depth: null });

          const ppfs = BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", itemData[0].returnData)[0] as ethers.BigNumber;
          const balance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", itemData[1].returnData)[0] as ethers.BigNumber;
          const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult(
            "getCurrentBlockTimestamp",
            itemData[2].returnData,
          )[0] as ethers.BigNumber;

          console.log({ ppfs: ppfs.toString(), balance: balance.toString(), blockTimestamp: new Date(blockTimestamp.toNumber() * 1000) });
        }
      }
    },
    formatOutput: options.formatOutput,
  });
}
