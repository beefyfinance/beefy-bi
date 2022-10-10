import { cloneDeep } from "lodash";
import { Range, rangeArrayExclude, rangeMerge, SupportedRangeTypes } from "../../../utils/range";

export interface ImportRanges<T extends SupportedRangeTypes> {
  // last time we imported some data for this product
  lastImportDate: Date;
  // already imported once range
  coveredRanges: Range<T>[];
  // ranges where an error occured
  toRetry: Range<T>[];
}

export function updateImportRanges<T extends SupportedRangeTypes>(
  _ranges: ImportRanges<T>,
  diff: { coveredRanges: Range<T>[]; successRanges: Range<T>[]; errorRanges: Range<T>[]; lastImportDate: Date },
): ImportRanges<T> {
  // make a copy before modifying anything
  const importRanges = cloneDeep(_ranges);

  // we need to remember which range we covered
  importRanges.coveredRanges = rangeMerge([...importRanges.coveredRanges, ...diff.coveredRanges]);

  // remove successes from the ranges we need to retry
  importRanges.toRetry = rangeArrayExclude(importRanges.toRetry, diff.successRanges);
  // add error ranges to the ranges we need to retry
  importRanges.toRetry = rangeMerge([...importRanges.toRetry, ...diff.errorRanges]);

  // update the last import date
  importRanges.lastImportDate = diff.lastImportDate;

  return importRanges;
}

export function hydrateNumberImportRangesFromDb(importRanges: ImportRanges<number>) {
  importRanges.lastImportDate = new Date(importRanges.lastImportDate);
}

export function hydrateDateImportRangesFromDb(importRanges: ImportRanges<Date>) {
  importRanges.lastImportDate = new Date(importRanges.lastImportDate);
  importRanges.coveredRanges = importRanges.coveredRanges.map((range) => ({
    from: new Date(range.from),
    to: new Date(range.to),
  }));
  importRanges.toRetry = importRanges.toRetry.map((range) => ({
    from: new Date(range.from),
    to: new Date(range.to),
  }));
}
