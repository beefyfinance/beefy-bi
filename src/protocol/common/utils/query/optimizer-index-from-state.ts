import { Range, SupportedRangeTypes, rangeMerge, rangeSplitManyToMaxLength, rangeToNumber, rangeValueMax } from "../../../../utils/range";

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
