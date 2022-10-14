import { isDate, isNumber } from "lodash";
import { Range, SupportedRangeTypes } from "../../../utils/range";

export interface ImportQuery<TTarget> {
  target: TTarget;
  range: Range<number>;
  latest: number;
}

export interface ImportResult<TTarget, TRange extends SupportedRangeTypes> {
  target: TTarget;
  range: Range<TRange>;
  latest: TRange;
  success: boolean;
}

export type ErrorEmitter<TObj, TQuery extends ImportQuery<TObj> = ImportQuery<TObj>> = (importQuery: TQuery) => void;

export function isDateRangeResult<TTarget>(result: ImportResult<TTarget, any>): result is ImportResult<TTarget, Date> {
  return isDate(result.range.from);
}
export function isBlockRangeResult<TTarget>(result: ImportResult<TTarget, any>): result is ImportResult<TTarget, number> {
  return isNumber(result.range.from);
}
