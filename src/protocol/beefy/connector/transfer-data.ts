import Decimal from "decimal.js";
import { ethers } from "ethers";
import { min } from "lodash";
import * as Rx from "rxjs";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { forkOnNullableField$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../../common/connector/block-datetime";
import { GetBalanceCallParams, fetchERC20TokenBalance$, vaultRawBalanceToBalance } from "../../common/connector/owner-balance";
import { fetchBlock$ } from "../../common/loader/blocks";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../common/types/import-context";
import { RPCBatchCallResult, batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import { BeefyPPFSCallParams, extractRawPpfsFromFunctionResult, fetchBeefyPPFS$, ppfsToVaultSharesRate } from "./ppfs";

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
}): Rx.OperatorFunction<TObj, TRes> {
  type MaybeResult = { result: { shareRate: Decimal; balance: Decimal; blockDatetime: Date | null } | null };
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
  );

  const maybeMulticallPipeline = Rx.pipe(
    Rx.tap((obj: TObj) =>
      logger.trace({ msg: "loading transfer data, maybe using multicall", data: { chain: options.ctx.chain, transferData: obj } }),
    ),
    Rx.map((obj) => ({ obj, param: options.getCallParams(obj) })),

    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      logInfos: { msg: "Fetching transfer data, maybe using multicall" },
      rpcCallsPerInputObj: {
        // this should be 1 but it's an heavy call so we virtually add 1 so batches are smaller
        // this lowers the probability that the RPC will timeout
        eth_call: 2,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: ({ param }) => param,
      processBatch: async (provider, params): Promise<RPCBatchCallResult<BeefyTransferCallParams, MaybeResult>> => {
        // find out if we can use multicall
        const minBlockNumber = min(params.map((c) => c.blockNumber)) || 0;
        if (!mcMap || mcMap.createdAtBlock >= minBlockNumber) {
          // we couldn't do multicall so we handle it later on
          logger.trace({ msg: "Could not use multicall, fallback to batch by call type", data: { mcMap, minBlockNumber } });
          return {
            successes: new Map(params.map((param) => [param, { result: null }] as const)),
            errors: new Map(),
          };
        }

        const multicallAddress = mcMap.multicallAddress;
        const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, provider);

        try {
          const batch = params.map((param) => {
            // read the next block for those chains who can't read their own writes
            let blockTag = param.blockNumber;
            if (!options.ctx.rpcConfig.rpcLimitations.stateChangeReadsOnSameBlock) {
              blockTag = param.blockNumber + 1;
            }
            const calls = [
              {
                allowFailure: false,
                callData: BeefyVaultV6AbiInterface.encodeFunctionData("balanceOf", [param.balance.ownerAddress]),
                target: param.balance.contractAddress,
              },
            ];
            if (options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
              calls.push({
                allowFailure: false,
                callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
                target: multicallAddress,
              });
            }
            if (param.fetchPpfs) {
              calls.push({
                allowFailure: false,
                callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
                target: param.ppfs.vaultAddress,
              });
            }
            return multicallContract.callStatic.aggregate3(calls, { blockTag });
          });

          const result: { success: boolean; returnData: string }[][] = await Promise.all(batch);

          const successResults = params.map((param, i) => {
            const r = result[i];

            let rIdx = 0;
            const rawBalance = BeefyVaultV6AbiInterface.decodeFunctionResult("balanceOf", r[rIdx++].returnData)[0] as ethers.BigNumber;
            const balance = vaultRawBalanceToBalance(param.balance.decimals, rawBalance);

            let blockDatetime: Date | null = null;
            if (options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
              const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult(
                "getCurrentBlockTimestamp",
                r[rIdx++].returnData,
              )[0] as ethers.BigNumber;
              blockDatetime = new Date(blockTimestamp.toNumber() * 1000);
            } else {
              logger.debug({
                msg: "RPC is not able to use Multicall3.getCurrentBlockTimestamp, fetching it later",
                data: { chain: options.ctx.chain },
              });
            }

            let shareRate = new Decimal(1);
            if (param.fetchPpfs) {
              const rawPpfs = extractRawPpfsFromFunctionResult(
                BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", r[rIdx++].returnData),
              ) as ethers.BigNumber;
              shareRate = ppfsToVaultSharesRate(param.ppfs.vaultDecimals, param.ppfs.underlyingDecimals, rawPpfs);
            }

            return [param, { result: { balance, blockDatetime, shareRate } }] as const;
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
      formatOutput: (item, res) => ({ ...item, ...res }),
    }),

    // handle those calls where multicall is available but was not created yet
    forkOnNullableField$({
      key: "result",
      handleNulls$: Rx.pipe(
        Rx.tap((item) => logger.trace({ msg: "Could not use multicall, fallback to no-multicall method", data: { mcMap, item } })),
        Rx.map((item) => item.obj),
        noMulticallPipeline,
      ),
      handleNonNulls$: Rx.pipe(Rx.map(({ obj, param, result }) => ({ obj, param, ...result }))),
    }),

    // handle those chains where multicall is not able to fetch the datetime
    forkOnNullableField$({
      key: "blockDatetime",
      handleNulls$: Rx.pipe(
        Rx.tap((item) =>
          logger.trace({ msg: "Could not use Multicall3.getCurrentBlockTimestamp, fetching the proper timestamp", data: { mcMap, item } }),
        ),
        fetchBlockDatetime$({
          ctx: options.ctx,
          emitError: (item, errReport) => options.emitError(item.obj, errReport),
          getBlockNumber: (item) => item.param.blockNumber,
          formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
        }),
      ),
      // sometimes the multicall result differ from other methods so if the block exists we
      // want to use the same datetime we already used in the database to avoid duplicate rows
      handleNonNulls$: Rx.pipe(
        fetchBlock$({
          ctx: options.ctx,
          chain: options.ctx.chain,
          emitError: (item, errReport) => options.emitError(item.obj, errReport),
          getBlockNumber: (item) => item.param.blockNumber,
          formatOutput: (item, block) => ({
            ...item,
            blockDatetime: block ? block.datetime : item.blockDatetime,
          }),
        }),
      ),
    }),

    Rx.map((item) => options.formatOutput(item.obj, { balance: item.balance, blockDatetime: item.blockDatetime, shareRate: item.shareRate })),
  );

  if (!mcMap) {
    return Rx.pipe(
      noMulticallPipeline,
      Rx.map((item) => options.formatOutput(item.obj, { balance: item.balance, blockDatetime: item.blockDatetime, shareRate: item.shareRate })),
    );
  } else {
    return maybeMulticallPipeline;
  }
}
