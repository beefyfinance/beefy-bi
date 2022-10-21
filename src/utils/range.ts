import { clone, cloneDeep, isDate, isNumber } from "lodash";
import { ProgrammerError } from "./programmer-error";

export type SupportedRangeTypes = number | Date;

export interface Range<T extends SupportedRangeTypes> {
  from: T;
  to: T;
}

interface RangeStrategy<T extends SupportedRangeTypes> {
  nextValue: (value: T) => T;
  previousValue: (value: T) => T;
  compare: (a: T, b: T) => number;
  max: (a: T, b: T) => T;
  diff: (a: T, b: T) => T;
  add: (a: T, b: number) => T;
}
const rangeStrategies = {
  number: {
    nextValue: (value: number) => value + 1,
    previousValue: (value: number) => value - 1,
    compare: (a: number, b: number) => a - b,
    max: (a: number, b: number) => Math.max(a, b),
    diff: (a: number, b: number) => a - b,
    add: (a: number, b: number) => a + b,
  } as RangeStrategy<number>,
  date: {
    nextValue: (value: Date) => new Date(value.getTime() + 1),
    previousValue: (value: Date) => new Date(value.getTime() - 1),
    compare: (a: Date, b: Date) => a.getTime() - b.getTime(),
    max: (a: Date, b: Date) => (a.getTime() > b.getTime() ? a : b),
    diff: (a: Date, b: Date) => new Date(a.getTime() - b.getTime()),
    add: (a: Date, b: number) => new Date(a.getTime() + b),
  } as RangeStrategy<Date>,
};

// some type guards
export function isDateRange(range: Range<any>): range is Range<Date> {
  return isDate(range.from);
}
export function isNumberRange(range: Range<any>): range is Range<number> {
  return isNumber(range.from);
}

function checkRange<T extends SupportedRangeTypes>(range: Range<T>, strategy?: RangeStrategy<T>) {
  const strat = strategy || getRangeStrategy(range);
  if (strat.compare(range.from, range.to) > 0) {
    throw new ProgrammerError("Range is invalid: from > to");
  }
}

function getRangeStrategy<T extends SupportedRangeTypes>(range: Range<T>): RangeStrategy<T> {
  const val = range.from;
  if (isDate(val)) {
    return rangeStrategies.date as any as RangeStrategy<T>;
  } else if (typeof val === "number") {
    return rangeStrategies.number as any as RangeStrategy<T>;
  } else {
    throw new ProgrammerError("Unsupported range type: " + typeof val);
  }
}

export function isInRange<T extends SupportedRangeTypes>(range: Range<T>, value: T, strategy?: RangeStrategy<T>): boolean {
  const strat = strategy || getRangeStrategy(range);
  return strat.compare(range.from, value) <= 0 && strat.compare(value, range.to) <= 0;
}

export function rangeValueMax<T extends SupportedRangeTypes>(values: T[], strategy?: RangeStrategy<T>): T | undefined {
  if (values.length <= 0) {
    return undefined;
  }
  const strat = strategy || getRangeStrategy({ from: values[0], to: values[0] });

  return values.reduce(strat.max, values[0]);
}

export function rangeArrayExclude<T extends SupportedRangeTypes>(ranges: Range<T>[], exclude: Range<T>[], strategy?: RangeStrategy<T>) {
  const strat = strategy || (ranges.length > 0 ? getRangeStrategy(ranges[0]) : undefined);
  return ranges.flatMap((range) => rangeExcludeMany(range, exclude, strat));
}

export function rangeExcludeMany<T extends SupportedRangeTypes>(range: Range<T>, exclude: Range<T>[], strategy?: RangeStrategy<T>): Range<T>[] {
  const strat = strategy || getRangeStrategy(range);
  checkRange(range, strat);

  let ranges = [range];
  for (const ex of exclude) {
    for (let i = 0; i < ranges.length; i++) {
      const range = ranges[i];
      const exclusionRes = rangeExclude(range, ex, strat);

      // nothing was excluded so we keep the range
      if (exclusionRes.length === 1 && rangeEqual(exclusionRes[0], range, strat)) {
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

export function rangeEqual<T extends SupportedRangeTypes>(a: Range<T>, b: Range<T>, strategy?: RangeStrategy<T>) {
  const strat = strategy || getRangeStrategy(a);
  checkRange(a, strat);
  checkRange(b, strat);
  return strat.compare(a.from, b.from) === 0 && strat.compare(a.to, b.to) === 0;
}

export function rangeExclude<T extends SupportedRangeTypes>(range: Range<T>, exclude: Range<T>, strategy?: RangeStrategy<T>): Range<T>[] {
  const strat = strategy || getRangeStrategy(range);
  checkRange(range, strat);
  checkRange(exclude, strat);

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
      { from: range.from, to: strat.previousValue(exclude.from) },
      { from: strat.nextValue(exclude.to), to: range.to },
    ];
  }

  // exclusion overlaps the range start
  if (range.from >= exclude.from && range.to >= exclude.to) {
    return [{ from: strat.nextValue(exclude.to), to: range.to }];
  }

  // exclusion overlaps the range end
  if (range.from <= exclude.from && range.to <= exclude.to) {
    return [{ from: range.from, to: strat.previousValue(exclude.from) }];
  }

  return [];
}

export function rangeMerge<T extends SupportedRangeTypes>(ranges: Range<T>[], strategy?: RangeStrategy<T>): Range<T>[] {
  if (ranges.length <= 1) {
    return cloneDeep(ranges);
  }
  const strat = strategy || getRangeStrategy(ranges[0]);
  const sortedRanges = ranges.sort((a, b) => strat.compare(a.from, b.from));
  const mergedRanges: Range<T>[] = [];

  let currentMergedRange: Range<T> | null = null;
  for (const range of sortedRanges) {
    checkRange(range, strat);

    if (!currentMergedRange) {
      currentMergedRange = clone(range);
      continue;
    }

    if (strat.compare(range.from, currentMergedRange.from) === 0) {
      currentMergedRange.to = strat.max(currentMergedRange.to, range.to);
      continue;
    }

    if (range.from <= strat.nextValue(currentMergedRange.to)) {
      currentMergedRange.to = strat.max(currentMergedRange.to, range.to);
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

export function rangeSlitToMaxLength<T extends SupportedRangeTypes>(range: Range<T>, maxLength: number, strategy?: RangeStrategy<T>): Range<T>[] {
  const strat = strategy || getRangeStrategy(range);
  checkRange(range, strat);

  const ranges: Range<T>[] = [];
  let currentRange: Range<T> = clone(range);

  while (strat.nextValue(strat.diff(currentRange.to, currentRange.from)) > maxLength) {
    ranges.push({ from: currentRange.from, to: strat.previousValue(strat.add(currentRange.from, maxLength)) });
    currentRange.from = strat.add(currentRange.from, maxLength);
  }

  ranges.push(currentRange);

  return ranges;
}

export function rangeSplitManyToMaxLength<T extends SupportedRangeTypes>(
  ranges: Range<T>[],
  maxLength: number,
  strategy?: RangeStrategy<T>,
): Range<T>[] {
  const strat = strategy || (ranges.length > 0 ? getRangeStrategy(ranges[0]) : undefined);
  return ranges.flatMap((range) => rangeSlitToMaxLength(range, maxLength, strat));
}
