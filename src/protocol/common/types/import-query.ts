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
