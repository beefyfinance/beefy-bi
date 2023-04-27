import Decimal from "decimal.js";
import { ethers } from "ethers";
import { min, uniq } from "lodash";
import { Hex } from "../../../types/address";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { MulticallCallOneCallParamStructure } from "../../../utils/rpc/call";
import { RpcRequestBatch, jsonRpcBatchEthCall } from "../../../utils/rpc/jsonrpc-batch";
import { callLockProtectedRpc } from "../../../utils/shared-resources/shared-rpc";
import { vaultRawBalanceToBalance } from "../../common/connector/owner-balance";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import { ppfsToVaultSharesRate } from "./ppfs";

const logger = rootLogger.child({ module: "beefy", component: "transfer-data" });

type BeefyTransferCallParams = {
  vaultAddress: Hex;
  vaultDecimals: number;
  investorAddress: Hex;
  underlyingDecimals: number;
  blockNumber: number;
  
};

type BeefyTransferCallResult = { shareRate: Decimal; balance: Decimal; blockDatetime: Date };

/**
 * Fetch every single data we need to be associated with a transfer
 * @param options
 * @returns
 */
export function fetchBeefyTransferData$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends BeefyTransferCallParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPPFSCallParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, data: BeefyTransferCallResult) => TRes;
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
    processBatch: async (provider, params) => {
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
        const jsonRpcBatch: RpcRequestBatch<[MulticallCallOneCallParamStructure[]], { success: boolean; returnData: Hex }[], (typeof params)[0]> = [];

        for (const param of params) {
          // create the multicall function call
          const multicallData: MulticallCallOneCallParamStructure[] = [
            {
              allowFailure: false, // don't account for division by zero errors for now
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
            blockTag: param.blockNumber,
            originalRequest: param,
          });
        }

        try {
          const successResults: [TParams, BeefyTransferCallResult][] = [];
          const result = await callLockProtectedRpc(() => jsonRpcBatchEthCall({ url: provider.connection.url, requests: jsonRpcBatch }), {
            chain: options.ctx.chain,
            logInfos: { msg: "Fetching transfer data using batched multicall", data: { callCount: jsonRpcBatch.length } },
            provider: provider,
            rpcLimitations: options.ctx.rpcConfig.rpcLimitations,
            noLockIfNoLimit: true,
            maxTotalRetryMs: options.ctx.streamConfig.maxTotalRetryMs,
          });

          for (const [call, res] of result.entries()) {
            const param = call.originalRequest;
            const itemData = await res;

            const rawPpfs = BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", itemData[0].returnData)[0] as ethers.BigNumber;
            const shareRate = ppfsToVaultSharesRate(param.vaultDecimals, param.underlyingDecimals, rawPpfs);
            const rawBalance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", itemData[1].returnData)[0] as ethers.BigNumber;
            const balance = vaultRawBalanceToBalance(param.vaultDecimals, rawBalance);
            const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult(
              "getCurrentBlockTimestamp",
              itemData[2].returnData,
            )[0] as ethers.BigNumber;

            const blockDatetime = new Date(blockTimestamp.toNumber() * 1000);
            successResults.push([param, { balance, blockDatetime, shareRate }]);
          }

          return {
            successes: new Map(successResults),
            errors: new Map(),
          };
        } catch (err) {
          logger.error({ msg: "Error fetching transfer data with custom batch builder", data: { err } });
          logger.error(err);

          return {
            successes: new Map(),
            errors: new Map(
              params.map((p) => [
                p,
                {
                  error: err,
                  infos: { msg: "Error fetching transfer data with custom batch builder" },
                },
              ]),
            ),
          };
        }
      } else {
        // here, we don't have access to multicall so we'll have to make a single call per data per contract

        const jsonRpcBatch: RpcRequestBatch<[MulticallCallOneCallParamStructure[]], { success: boolean; returnData: Hex }[], (typeof params)[0]> = [];

        for (const param of params) {
          jsonRpcBatch.push({
            interface: BeefyVaultV6AbiInterface,
            contractAddress: param.vaultAddress,
            params: undefined,
            function: "getPricePerFullShare",
            blockTag: param.blockNumber,
            originalRequest: param,
          });

          jsonRpcBatch.push({
            interface: BeefyVaultV6AbiInterface,
            contractAddress: param.vaultAddress,
            params: [param.investorAddress],
            function: "balanceOf",
            blockTag: param.blockNumber,
            originalRequest: param,
          });

          jsonRpcBatch.push({
            interface: Multicall3AbiInterface,
            contractAddress: param.multicallAddress,
            params: undefined,
            function: "getCurrentBlockTimestamp",
            blockTag: param.blockNumber,
            originalRequest: param,
          });
        }
      }
    },
    formatOutput: options.formatOutput,
  });
}
