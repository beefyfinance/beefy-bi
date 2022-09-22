import { clone, cloneDeep } from "lodash";

interface Range {
  from: number;
  to: number;
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
