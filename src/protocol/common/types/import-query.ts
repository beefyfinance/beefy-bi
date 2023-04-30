import { isDate, isNumber } from "lodash";
import { Range, SupportedRangeTypes } from "../../../utils/range";

export interface ImportRangeQuery<TTarget, TRange extends SupportedRangeTypes> {
  target: TTarget;
  range: Range<TRange>;
  latest: TRange;
}

export interface ImportRangeResult<TTarget, TRange extends SupportedRangeTypes> {
  target: TTarget;
  range: Range<TRange>;
  latest: TRange;
  success: boolean;
}
export interface ImportRangeResultMaybeLatest<TTarget, TRange extends SupportedRangeTypes> {
  target: TTarget;
  range: Range<TRange>;
  latest?: TRange;
  success: boolean;
}
export function isDateRangeResult<TTarget>(result: ImportRangeResult<TTarget, any>): result is ImportRangeResult<TTarget, Date> {
  return isDate(result.range.from);
}
export function isBlockRangeResult<TTarget>(result: ImportRangeResult<TTarget, any>): result is ImportRangeResult<TTarget, number> {
  return isNumber(result.range.from);
}
