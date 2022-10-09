import { clone, cloneDeep } from "lodash";

export interface Range {
  from: number;
  to: number;
}

export function rangeArrayExclude(ranges: Range[], exclude: Range[]) {
  return ranges.flatMap((range) => rangeExcludeMany(range, exclude));
}

export function rangeExcludeMany(range: Range, exclude: Range[]): Range[] {
  let ranges = [range];
  for (const ex of exclude) {
    for (let i = 0; i < ranges.length; i++) {
      const range = ranges[i];
      const exclusionRes = rangeExclude(range, ex);

      // nothing was excluded so we keep the range
      if (exclusionRes.length === 1 && rangeEqual(exclusionRes[0], range)) {
        continue;
      }
      // the range was fully excluded so we remove it
      else if (exclusionRes.length === 0) {
        ranges.splice(i, 1);
        i--;
        // an exclusion was made so we replace the range with the exclusion result and retry
      } else {
        ranges.splice(i, 1, ...exclusionRes);
        i--;
      }
    }
  }
  return ranges;
}

export function rangeEqual(a: Range, b: Range) {
  return a.from === b.from && a.to === b.to;
}

export function rangeExclude(range: Range, exclude: Range): Range[] {
  // ranges are exclusives
  if (range.to < exclude.from) {
    return [clone(range)];
  }
  if (range.from > exclude.to) {
    return [clone(range)];
  }

  // exclusion fully contains the range
  if (range.from >= exclude.from && range.to <= exclude.to) {
    return [];
  }

  // exclusion is inside the range
  if (range.from < exclude.from && range.to > exclude.to) {
    return [
      { from: range.from, to: exclude.from - 1 },
      { from: exclude.to + 1, to: range.to },
    ];
  }

  // exclusion overlaps the range start
  if (range.from >= exclude.from && range.to >= exclude.to) {
    return [{ from: exclude.to + 1, to: range.to }];
  }

  // exclusion overlaps the range end
  if (range.from <= exclude.from && range.to <= exclude.to) {
    return [{ from: range.from, to: exclude.from - 1 }];
  }

  return [];
}

export function rangeMerge(ranges: Range[]): Range[] {
  if (ranges.length <= 1) {
    return cloneDeep(ranges);
  }
  const sortedRanges = ranges.sort((a, b) => a.from - b.from);
  const mergedRanges: Range[] = [];

  let currentMergedRange: Range | null = null;
  for (const range of sortedRanges) {
    if (!currentMergedRange) {
      currentMergedRange = clone(range);
      continue;
    }

    if (range.from === currentMergedRange.from) {
      currentMergedRange.to = Math.max(currentMergedRange.to, range.to);
      continue;
    }

    if (range.from <= currentMergedRange.to + 1) {
      currentMergedRange.to = Math.max(currentMergedRange.to, range.to);
      continue;
    }

    if (currentMergedRange) {
      mergedRanges.push(currentMergedRange);
      currentMergedRange = clone(range);
    }
  }

  if (currentMergedRange) {
    mergedRanges.push(currentMergedRange);
  }

  return mergedRanges;
}

export function rangeSlitToMaxLength(range: Range, maxLength: number): Range[] {
  const ranges: Range[] = [];
  let currentRange: Range = clone(range);

  while (currentRange.to - currentRange.from + 1 > maxLength) {
    ranges.push({ from: currentRange.from, to: currentRange.from + maxLength - 1 });
    currentRange.from += maxLength;
  }

  ranges.push(currentRange);

  return ranges;
}

export function rangeSplitManyToMaxLength(ranges: Range[], maxLength: number): Range[] {
  return ranges.flatMap((range) => rangeSlitToMaxLength(range, maxLength));
}
