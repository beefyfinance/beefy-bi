import { isArray, keyBy, max, sortBy } from "lodash";
import { SamplingPeriod, samplingPeriodMs } from "../../../../types/sampling";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, SupportedRangeTypes, rangeMerge, rangeSplitManyToMaxLength, rangeToNumber, rangeValueMax } from "../../../../utils/range";
import { AddressBatchOutput, JsonRpcBatchOutput, OptimizerInput, QueryOptimizerOutput, StrategyInput, StrategyResult } from "./query-types";

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

export function getLoggableOptimizerOutput<TObj, TRange extends SupportedRangeTypes>(
  input: OptimizerInput<TObj, TRange> | StrategyInput<TObj, TRange>,
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

export function getLoggableInput<TObj, TRange extends SupportedRangeTypes>(input: OptimizerInput<TObj, TRange> | StrategyInput<TObj, TRange>): any {
  return { ...input, states: input.states.map((s) => ({ ...s, obj: input.objKey(s.obj) })) };
}

/**
 * Split the total range to cover into consecutive blobs that should be handled independently
 */
export function createOptimizerIndexFromState<TRange extends SupportedRangeTypes>(
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

export function blockNumberListToRanges({
  blockNumberList,
  latestBlockNumber,
  snapshotInterval,
  maxBlocksPerQuery,
  msPerBlockEstimate,
}: {
  blockNumberList: number[];
  latestBlockNumber: number;
  maxBlocksPerQuery: number;
  snapshotInterval: SamplingPeriod;
  msPerBlockEstimate: number;
}): Range<number>[] {
  const blockRanges: Range<number>[] = [];
  const sortedBlockNumbers = sortBy(blockNumberList);
  for (let i = 0; i < sortedBlockNumbers.length - 1; i++) {
    const block = sortedBlockNumbers[i];
    const nextBlock = blockNumberList[i + 1];
    blockRanges.push({ from: block, to: nextBlock - 1 });
  }
  // add a range between last db block and latest block
  if (blockRanges.length === 0) {
    return [];
  }
  const maxDbBlock = max(blockNumberList) as number;

  blockRanges.push({ from: maxDbBlock + 1, to: latestBlockNumber });

  // split ranges in chunks of ~15min
  const maxTimeStepMs = samplingPeriodMs[snapshotInterval];
  const avgBlockPerTimeStep = Math.floor(maxTimeStepMs / msPerBlockEstimate);
  const rangeMaxLength = Math.min(avgBlockPerTimeStep, maxBlocksPerQuery);

  return rangeSplitManyToMaxLength(blockRanges, rangeMaxLength);
}
