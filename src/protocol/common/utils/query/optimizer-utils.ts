import { isArray, keyBy } from "lodash";
import { RpcConfig } from "../../../../types/rpc-config";
import { samplingPeriodMs } from "../../../../types/sampling";
import { MS_PER_BLOCK_ESTIMATE } from "../../../../utils/config";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import {
  Range,
  SupportedRangeTypes,
  isValidRange,
  rangeMerge,
  rangeSplitManyToMaxLength,
  rangeToNumber,
  rangeValueMax,
} from "../../../../utils/range";
import { DbBlockNumberRangeImportState } from "../../loader/import-state";
import { ImportBehaviour } from "../../types/import-context";
import {
  AddressBatchOutput,
  JsonRpcBatchOutput,
  QueryOptimizerOutput,
  RangeQueryOptimizerInput,
  RangeQueryOptimizerOptions,
  SnapshotQueryOptimizerOptions,
  StrategyInput,
  StrategyResult,
} from "./query-types";

const logger = rootLogger.child({ module: "common", component: "query-optimizer-utils" });

// some type guards and accessors
export function isJsonRpcBatchQueries<TObj, TRange extends SupportedRangeTypes>(
  o: QueryOptimizerOutput<TObj, TRange>,
): o is JsonRpcBatchOutput<TObj, TRange> {
  return o.type === "jsonrpc batch";
}
export function isAddressBatchQueries<TObj, TRange extends SupportedRangeTypes>(
  o: QueryOptimizerOutput<TObj, TRange>,
): o is AddressBatchOutput<TObj, TRange> {
  return o.type === "address batch";
}

export function extractObjsAndRangeFromOptimizerOutput<TObj, TRange extends SupportedRangeTypes>({
  output,
  objKey,
}: {
  objKey: (obj: TObj) => string;
  output: QueryOptimizerOutput<TObj, TRange>;
}): { obj: TObj; range: Range<TRange> }[] {
  if (isJsonRpcBatchQueries(output)) {
    return [{ obj: output.obj, range: output.range }];
  } else if (isAddressBatchQueries(output)) {
    const postfiltersByObj = keyBy(output.postFilters, (pf) => objKey(pf.obj));
    return output.objs.flatMap((obj) => {
      const postFilter = postfiltersByObj[objKey(obj)];
      if (postFilter && postFilter.filter !== "no-filter") {
        return postFilter.filter.map((range) => ({ obj: postFilter.obj, range }));
      } else {
        return { obj, range: output.range };
      }
    });
  } else {
    throw new ProgrammerError("Unsupported type of optimizer output: " + output);
  }
}

export function getLoggableOptimizerOutput<
  TObj,
  TOptions extends RangeQueryOptimizerOptions | SnapshotQueryOptimizerOptions,
  TRange extends SupportedRangeTypes,
>(
  input: RangeQueryOptimizerInput<TObj, TRange> | StrategyInput<TObj, TOptions, TRange>,
  output: (QueryOptimizerOutput<TObj, TRange> | StrategyResult<QueryOptimizerOutput<TObj, TRange>>) | QueryOptimizerOutput<TObj, TRange>[],
): any {
  if (isArray(output)) {
    return output.map((o) => getLoggableOptimizerOutput(input, o));
  }
  if ("totalCoverage" in output) {
    return { ...output, result: getLoggableOptimizerOutput(input, output.result) };
  }
  if (isAddressBatchQueries(output)) {
    return {
      ...output,
      objs: output.objs.map(input.objKey),
      postFilters: output.postFilters.map((f) => ({ ...f, obj: input.objKey(f.obj) })),
    };
  } else {
    return { ...output, obj: input.objKey(output.obj) };
  }
}

export function getLoggableInput<
  TObj,
  TOptions extends RangeQueryOptimizerOptions | SnapshotQueryOptimizerOptions,
  TRange extends SupportedRangeTypes,
>(input: RangeQueryOptimizerInput<TObj, TRange> | StrategyInput<TObj, TOptions, TRange>) {
  return { ...input, states: input.states.map((s) => ({ ...s, obj: input.objKey(s.obj) })) };
}

/**
 * Split the total range to cover into consecutive blobs that should be handled independently
 */
export function _buildRangeIndex<TRange extends SupportedRangeTypes>(
  input: Range<TRange>[][],
  { mergeIfCloserThan, verticalSlicesSize }: { mergeIfCloserThan: number; verticalSlicesSize: number },
): Range<TRange>[] {
  const ranges = rangeMerge(input.flatMap((s) => s));
  if (ranges.length <= 1) {
    return rangeSplitManyToMaxLength(ranges, verticalSlicesSize);
  }

  // merge the index if the ranges are "close enough"
  const res: Range<TRange>[] = [];
  // we take advantage of the ranges being sorted after merge
  let buildUp = ranges.shift() as Range<TRange>;
  while (ranges.length > 0) {
    const currentRange = ranges.shift() as Range<TRange>;
    const bn = rangeToNumber(buildUp);
    const cn = rangeToNumber(currentRange);

    // merge if possible
    if (bn.to + mergeIfCloserThan >= cn.from) {
      buildUp.to = rangeValueMax([currentRange.to, buildUp.to]) as TRange;
    } else {
      // otherwise we changed blob
      res.push(buildUp);
      buildUp = currentRange;
    }
  }
  res.push(buildUp);

  // now split into vertical slices
  return rangeSplitManyToMaxLength(res, verticalSlicesSize);
}

export function importStateToOptimizerRangeInput({
  behaviour,
  rpcConfig,
  lastImportedBlockNumber,
  isLive,
  latestBlockNumber,
  importState,
  logInfos,
}: {
  behaviour: ImportBehaviour;
  rpcConfig: RpcConfig;
  latestBlockNumber: number;
  lastImportedBlockNumber: number | null;
  importState: DbBlockNumberRangeImportState | null;
  isLive: boolean;
  logInfos: LogInfos;
}) {
  // compute recent full range in case we need it
  // fetch the last hour of data
  const period = samplingPeriodMs["1day"];
  const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[rpcConfig.chain]);

  const diffBetweenLastImported = lastImportedBlockNumber
    ? Math.max(0, latestBlockNumber - (lastImportedBlockNumber + behaviour.waitForBlockPropagation + 1))
    : Infinity;

  const maxBlocksPerQuery = rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
  const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
  const fromBlock = latestBlockNumber - blockCountToFetch;
  const toBlock = latestBlockNumber;
  const recentFullRange = {
    from: fromBlock - behaviour.waitForBlockPropagation,
    to: toBlock - behaviour.waitForBlockPropagation,
  };

  let fullRange: Range<number>;

  if (behaviour.mode !== "recent" && importState !== null) {
    // exclude latest block query from the range
    const skipRecent = behaviour.skipRecentWindowWhenHistorical;
    let doSkip = false;
    if (skipRecent === "all") {
      doSkip = true;
    } else if (skipRecent === "none") {
      doSkip = false;
    } else if (skipRecent === "live") {
      doSkip = isLive;
    } else if (skipRecent === "eol") {
      doSkip = !isLive;
    } else {
      throw new ProgrammerError({ msg: "Invalid skipRecentWindowWhenHistorical value", data: { skipRecent } });
    }
    // this is the whole range we have to cover
    fullRange = {
      from: importState.importData.contractCreatedAtBlock,
      to: Math.min(latestBlockNumber - behaviour.waitForBlockPropagation, doSkip ? recentFullRange.to : Infinity),
    };
  } else {
    fullRange = recentFullRange;
  }

  // this can happen when we force the block number in the past and we are treating a recent product
  if (fullRange.from > fullRange.to) {
    const importStateKey = importState?.importKey;
    if (behaviour.forceConsideredBlockRange !== null) {
      logger.info(
        mergeLogsInfos(
          {
            msg: "current block number set too far in the past to treat this product",
            data: { fullRange, importStateKey },
          },
          logInfos,
        ),
      );
    } else {
      logger.error(
        mergeLogsInfos(
          {
            msg: "Full range is invalid",
            data: { fullRange, importStateKey },
          },
          logInfos,
        ),
      );
      if (process.env.NODE_ENV === "development") {
        throw new ProgrammerError("Full range is invalid");
      }
    }
  }

  const coveredRanges = behaviour.ignoreImportState ? [] : importState?.importData.ranges.coveredRanges || [];
  let toRetry = !behaviour.ignoreImportState && behaviour.mode === "historical" && importState !== null ? importState.importData.ranges.toRetry : [];

  // apply our range restriction everywhere
  if (behaviour.forceConsideredBlockRange !== null) {
    const restrict = behaviour.forceConsideredBlockRange;
    fullRange = {
      from: Math.max(fullRange.from, restrict.from),
      to: Math.min(fullRange.to, restrict.to),
    };
    toRetry = rangeMerge(
      toRetry
        .map((range) => ({
          from: Math.max(range.from, restrict.from),
          to: Math.min(range.to, restrict.to),
        }))
        .filter((r) => isValidRange(r)),
    );
  }

  return { fullRange, coveredRanges, toRetry };
}
