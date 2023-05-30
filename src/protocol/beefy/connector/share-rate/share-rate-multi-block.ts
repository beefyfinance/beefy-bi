import Decimal from "decimal.js";
import { ethers } from "ethers";
import { min } from "lodash";
import * as Rx from "rxjs";
import { BeefyVaultV6AbiInterface, Multicall3AbiInterface } from "../../../../utils/abi";
import { MULTICALL3_ADDRESS_MAP } from "../../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { forkOnNullableField$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../../common/types/import-context";
import { RPCBatchCallResult, batchRpcCalls$ } from "../../../common/utils/batch-rpc-calls";
import {
  BeefyShareRateBatchCallResult,
  BeefyShareRateCallParams,
  extractRawPpfsFromFunctionResult,
  getBlockTag,
  isEmptyVaultPPFSError,
  ppfsToVaultSharesRate,
} from "./share-rate-utils";

const logger = rootLogger.child({ module: "beefy", component: "share-rate" });

/**
 * This is for fetching multiple share rate with distinct block number
 * While fetching investments for ex.
 * This is not efficient for ppfs snapshoting where we can do way better
 */
export function fetchSingleBeefyProductShareRateAndDatetime$<TObj, TErr extends ErrorEmitter<TObj>, TRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getCallParams: (obj: TObj) => (BeefyShareRateCallParams & { type: "fetch" }) | { type: "set-to-1"; blockNumber: number };
  formatOutput: (obj: TObj, result: BeefyShareRateBatchCallResult) => TRes;
}) {
  const logInfos = { msg: "Fetching Beefy share rate single mode", data: { chain: options.ctx.chain } };
  const mcMap = MULTICALL3_ADDRESS_MAP[options.ctx.chain];
  type MaybeResult = { result: { blockNumber: number; shareRate: Decimal; blockDatetime: Date } | null };

  const noMulticallPipeline = Rx.pipe(
    Rx.tap(({ obj }: { obj: TObj; param: BeefyShareRateCallParams }) =>
      logger.trace(mergeLogsInfos({ msg: "no multicall", data: { transferData: obj } }, logInfos)),
    ),

    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, report) => options.emitError(item.obj, report),
      logInfos: mergeLogsInfos({ msg: "maybe using multicall" }, logInfos),
      rpcCallsPerInputObj: {
        eth_call: 1,
        eth_blockNumber: 0,
        eth_getBlockByNumber: 0,
        eth_getLogs: 0,
        eth_getTransactionReceipt: 0,
      },
      getQuery: (item) => item.param,
      processBatch: async (provider, params) => {
        // fetch all ppfs in one go, this will batch calls using jsonrpc batching
        type PPFSEntry = [BeefyShareRateCallParams, ethers.BigNumber];
        type MapEntry = [BeefyShareRateCallParams, Decimal];
        type ErrEntry = [BeefyShareRateCallParams, ErrorReport];
        type CallResult<T> = { type: "success"; data: T } | { type: "error"; data: ErrEntry };
        let shareRatePromises: Promise<CallResult<MapEntry>>[] = [];
        for (const contractCall of params) {
          let rawPromise: Promise<[ethers.BigNumber]>;

          const contract = new ethers.Contract(contractCall.vaultAddress, BeefyVaultV6AbiInterface, provider);
          rawPromise = contract.callStatic.getPricePerFullShare({ blockTag: getBlockTag(options.ctx, contractCall.blockNumber) });

          const shareRatePromise = rawPromise
            .then(
              (ppfs): CallResult<PPFSEntry> => ({
                type: "success",
                data: [contractCall, extractRawPpfsFromFunctionResult(ppfs)] as PPFSEntry,
              }),
            )
            // empty vaults WILL throw an error
            .catch((err): CallResult<PPFSEntry> => {
              if (isEmptyVaultPPFSError(err)) {
                return { type: "success", data: [contractCall, ethers.BigNumber.from(0)] as PPFSEntry };
              } else {
                return {
                  type: "error",
                  data: [contractCall, { error: err, infos: { msg: "Unrecoverable error while fetching ppfs", data: { contractCall } } }] as ErrEntry,
                };
              }
            })
            .then((res): CallResult<MapEntry> => {
              if (res.type === "success") {
                const vaultShareRate = ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, res.data[1]);
                return { type: "success", data: [contractCall, vaultShareRate] };
              } else if (res.type === "error") {
                return res;
              } else {
                throw new ProgrammerError({ msg: "Unmapped type", data: { res } });
              }
            });

          shareRatePromises.push(shareRatePromise);
        }

        const batchResults = await Promise.all(shareRatePromises);
        return {
          successes: new Map(batchResults.filter((r): r is { type: "success"; data: MapEntry } => r.type === "success").map((r) => r.data)),
          errors: new Map(batchResults.filter((r): r is { type: "error"; data: ErrEntry } => r.type === "error").map((r) => r.data)),
        };
      },
      formatOutput: (item, shareRate) => ({ ...item, shareRate, blockNumber: getBlockTag(options.ctx, item.param.blockNumber) }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      getBlockNumber: (item) => item.param.blockNumber,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),
  );

  const multicallPipeline = Rx.pipe(
    Rx.tap(({ obj }: { obj: TObj; param: BeefyShareRateCallParams }) =>
      logger.trace(mergeLogsInfos({ msg: "maybe using multicall", data: { transferData: obj } }, logInfos)),
    ),

    batchRpcCalls$({
      ctx: options.ctx,
      emitError: (item, errReport) => options.emitError(item.obj, errReport),
      logInfos: mergeLogsInfos({ msg: "maybe using multicall" }, logInfos),
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
      processBatch: async (provider, params): Promise<RPCBatchCallResult<BeefyShareRateCallParams, MaybeResult>> => {
        // find out if we can use multicall
        const minBlockNumber = min(params.map((c) => c.blockNumber)) || 0;
        if (!mcMap || mcMap.createdAtBlock >= minBlockNumber) {
          // we couldn't do multicall so we handle it later on
          logger.trace(mergeLogsInfos({ msg: "Could not use multicall, fallback to batch by call type", data: { mcMap, minBlockNumber } }, logInfos));
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
            const blockTag = getBlockTag(options.ctx, param.blockNumber);
            const calls = [
              {
                allowFailure: true,
                callData: BeefyVaultV6AbiInterface.encodeFunctionData("getPricePerFullShare"),
                target: param.vaultAddress,
              },
              {
                allowFailure: false,
                callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
                target: multicallAddress,
              },
            ];
            return multicallContract.callStatic.aggregate3(calls, { blockTag });
          });

          const result: { success: boolean; returnData: string }[][] = await Promise.all(batch);

          const successResults = params.map((param, i) => {
            const r = result[i];

            let rIdx = 0;

            const ppfsData = r[rIdx++];
            // if the call failed, we immediately know it was an empty vault, we don't get the error back anyway
            let shareRate = new Decimal("1");
            if (ppfsData.success) {
              const rawPpfs = extractRawPpfsFromFunctionResult(
                BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", ppfsData.returnData),
              ) as ethers.BigNumber;
              shareRate = ppfsToVaultSharesRate(param.vaultDecimals, param.underlyingDecimals, rawPpfs);
            }

            let blockDatetime: Date | null = null;
            const blockTimestamp = Multicall3AbiInterface.decodeFunctionResult(
              "getCurrentBlockTimestamp",
              r[rIdx++].returnData,
            )[0] as ethers.BigNumber;
            blockDatetime = new Date(blockTimestamp.toNumber() * 1000);

            return [param, { result: { blockNumber: getBlockTag(options.ctx, param.blockNumber), blockDatetime, shareRate } }] as const;
          });

          return {
            successes: new Map(successResults),
            errors: new Map(),
          };
        } catch (err: any) {
          logger.error(mergeLogsInfos({ msg: "Error fetching transfer data with custom batch builder", data: { err } }, logInfos));
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
        Rx.tap((item) =>
          logger.trace(mergeLogsInfos({ msg: "Could not use multicall, fallback to no-multicall method", data: { mcMap, item } }, logInfos)),
        ),
        Rx.map((item) => ({ obj: item.obj, param: item.param })),
        noMulticallPipeline,
      ),
      handleNonNulls$: Rx.pipe(Rx.map(({ obj, param, result }) => ({ obj, param, ...result }))),
    }),
    Rx.map((item) => options.formatOutput(item.obj, { blockNumber: item.blockNumber, blockDatetime: item.blockDatetime, shareRate: item.shareRate })),
  );

  let strategy = multicallPipeline;
  // if there is no multicall or if multicall has broken blockTimestamp, use no multicall
  if (!mcMap || !options.ctx.rpcConfig.rpcLimitations.canUseMulticallBlockTimestamp) {
    strategy = Rx.pipe(
      noMulticallPipeline,
      Rx.map((item) =>
        options.formatOutput(item.obj, { blockNumber: item.blockNumber, blockDatetime: item.blockDatetime, shareRate: item.shareRate }),
      ),
    );
  }

  return Rx.pipe(
    Rx.tap((obj: TObj) => logger.trace(mergeLogsInfos({ msg: "forking on product type", data: { transferData: obj } }, logInfos))),
    Rx.map((obj) => ({ obj, param: options.getCallParams(obj) })),
    // handle different types of requests differently
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; param: BeefyShareRateCallParams & { type: "fetch" } } => item.param.type === "fetch"),
          strategy,
        ),
        items$.pipe(
          Rx.filter((item): item is { obj: TObj; param: { type: "set-to-1"; blockNumber: number } } => item.param.type === "set-to-1"),
          fetchBlockDatetime$({
            ctx: options.ctx,
            emitError: (item, report) => options.emitError(item.obj, report),
            getBlockNumber: ({ param: { blockNumber } }) => blockNumber,
            formatOutput: (item, blockDatetime) => ({ obj: item.obj, blockNumber: item.param.blockNumber, blockDatetime }),
          }),
          Rx.map((item) =>
            options.formatOutput(item.obj, { blockNumber: item.blockNumber, blockDatetime: item.blockDatetime, shareRate: new Decimal(1) }),
          ),
        ),
      ),
    ),
  );
}
