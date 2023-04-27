import Decimal from "decimal.js";
import { ethers } from "ethers";
import { min } from "lodash";
import * as Rx from "rxjs";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { fetchBlockDatetime$ } from "../../common/connector/block-datetime";
import { GetBalanceCallParams, fetchERC20TokenBalance$, vaultRawBalanceToBalance } from "../../common/connector/owner-balance";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../common/types/import-context";
import { batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import { BeefyPPFSCallParams, fetchBeefyPPFS$, ppfsToVaultSharesRate } from "./ppfs";

const logger = rootLogger.child({ module: "beefy", component: "transfer-data" });

interface NoPpfsCallParams {
  balance: Omit<GetBalanceCallParams, "blockNumber">;
  blockNumber: number;
  fetchPpfs: false;
}
interface WithPpfsCallParams {
  ppfs: Omit<BeefyPPFSCallParams, "blockNumber">;
  balance: Omit<GetBalanceCallParams, "blockNumber">;
  blockNumber: number;
  fetchPpfs: true;
}
type BeefyTransferCallParams = NoPpfsCallParams | WithPpfsCallParams;

type BeefyTransferCallResult = { shareRate: Decimal; balance: Decimal; blockDatetime: Date };

/**
 * Fetch every single data we need to be associated with a transfer
 * @param options
 * @returns
 */
export function fetchBeefyTransferData$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getCallParams: (obj: TObj) => BeefyTransferCallParams;
  formatOutput: (obj: TObj, data: BeefyTransferCallResult) => TRes;
}) {
  const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];

  const noMulticallPipeline = Rx.pipe(
    Rx.tap((obj: TObj) => logger.trace({ msg: "loading transfer data, no multicall", data: { chain: options.ctx.chain, transferData: obj } })),
    Rx.map((obj) => ({ obj, param: options.getCallParams(obj) })),

    // fetch the ppfs if needed
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; param: WithPpfsCallParams } => item.param.fetchPpfs),
          fetchBeefyPPFS$({
            ctx: options.ctx,
            emitError: (item, errReport) => options.emitError(item.obj, errReport),
            getPPFSCallParams: (item) => {
              return {
                vaultAddress: item.param.ppfs.vaultAddress,
                underlyingDecimals: item.param.ppfs.underlyingDecimals,
                vaultDecimals: item.param.ppfs.vaultDecimals,
                blockNumber: item.param.blockNumber,
              };
            },
            formatOutput: (item, shareRate) => ({ ...item, shareRate }),
          }),
        ),
        items$.pipe(
          Rx.filter((item) => !item.param.fetchPpfs),
          Rx.map((item) => ({ ...item, shareRate: new Decimal(1) })),
        ),
      ),
    ),

    // we need the balance of each owner
    fetchERC20TokenBalance$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      getQueryParams: (item) => ({
        blockNumber: item.param.blockNumber,
        decimals: item.param.balance.decimals,
        contractAddress: item.param.balance.contractAddress,
        ownerAddress: item.param.balance.ownerAddress,
      }),
      formatOutput: (item, balance) => ({ ...item, balance }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      getBlockNumber: (item) => item.param.blockNumber,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    Rx.map((item) => options.formatOutput(item.obj, { balance: item.balance, blockDatetime: item.blockDatetime, shareRate: item.shareRate })),
  );

  type CouldDoMulticallResult = BeefyTransferCallResult & { couldMulticall: true };
  type CouldNotDoMulticallResult = { couldMulticall: false };
  type MaybeMulticallResult = CouldDoMulticallResult | CouldNotDoMulticallResult;
  const maybeMulticallPipeline = Rx.pipe(
    Rx.tap((obj: TObj) =>
      logger.trace({ msg: "loading transfer data, maybe using multicall", data: { chain: options.ctx.chain, transferData: obj } }),
    ),

    batchRpcCalls$<TObj, TErr, { obj: TObj } & MaybeMulticallResult, BeefyTransferCallParams, MaybeMulticallResult>({
      ctx: options.ctx,
      emitError: options.emitError,
      logInfos: { msg: "Fetching transfer data, maybe using multicall" },
      rpcCallsPerInputObj: {
        eth_call: 3,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: options.getCallParams,
      processBatch: async (provider, params) => {
        // find out if we can use multicall
        const minBlockNumber = min(params.map((c) => c.blockNumber)) || 0;
        if (!mcMap || mcMap.createdAtBlock >= minBlockNumber) {
          // we couldn't do multicall so we handle it later on
          logger.trace({ msg: "Could not use multicall, fallback to batch by call type", data: { mcMap, minBlockNumber } });
          return {
            successes: new Map(params.map((param) => [param, { couldMulticall: false }] as const)),
            errors: new Map(),
          };
        }

        const multicallAddress = mcMap.multicallAddress;
        const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);

        try {
          const batch = params.map((param) => {
            const calls = [
              {
                allowFailure: false,
                callData: BeefyVaultV6AbiInterface.encodeFunctionData("balanceOf", [param.balance.ownerAddress]),
                target: param.balance.contractAddress,
              },
              {
                allowFailure: false,
                callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
                target: multicallAddress,
              },
            ];
            if (param.fetchPpfs) {
              calls.push({
                allowFailure: false,
                callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
                target: param.ppfs.vaultAddress,
              });
            }
            return multicallContract.callStatic.aggregate3(calls, { blockTag: param.blockNumber });
          });

          const result: { success: boolean; returnData: string }[][] = await Promise.all(batch);

          const successResults = params.map((param, i) => {
            const r = result[i];

            const rawBalance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", r[0].returnData)[0] as ethers.BigNumber;
            const balance = vaultRawBalanceToBalance(param.balance.decimals, rawBalance);
            const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult("getCurrentBlockTimestamp", r[1].returnData)[0] as ethers.BigNumber;
            const blockDatetime = new Date(blockTimestamp.toNumber() * 1000);

            let shareRate = new Decimal(1);
            if (param.fetchPpfs) {
              const rawPpfs = BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", r[2].returnData)[0] as ethers.BigNumber;
              shareRate = ppfsToVaultSharesRate(param.ppfs.vaultDecimals, param.ppfs.underlyingDecimals, rawPpfs);
            }

            return [param, { couldMulticall: true, balance, blockDatetime, shareRate }] as const;
          });

          return {
            successes: new Map(successResults),
            errors: new Map(),
          };
        } catch (err: any) {
          logger.error({ msg: "Error fetching transfer data with custom batch builder", data: { err } });
          logger.error(err);
          const report: ErrorReport = { error: err, infos: { msg: "Error fetching transfer data with custom batch builder" } };

          return {
            successes: new Map(),
            errors: new Map(params.map((p) => [p, report] as const)),
          };
        }
      },
      formatOutput: (obj, res) => ({ obj, ...res }),
    }),

    // handle those calls where multicall is available but was not created yet
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj } & CouldNotDoMulticallResult => !item.couldMulticall),
          Rx.tap((item) => logger.trace({ msg: "Could not use multicall, fallback to batch by call type", data: { mcMap, item } })),
          Rx.map((item) => item.obj),
          noMulticallPipeline,
        ),
        items$.pipe(
          Rx.filter((item): item is { obj: TObj } & CouldDoMulticallResult => item.couldMulticall),
          Rx.map((item) => options.formatOutput(item.obj, item)),
        ),
      ),
    ),
  );

  if (!mcMap) {
    return noMulticallPipeline;
  } else {
    return maybeMulticallPipeline;
  }
}
