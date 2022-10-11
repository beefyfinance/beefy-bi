import { Range } from "../../../utils/range";

export interface ImportQuery<TTarget> {
  target: TTarget;
  blockRange: Range<number>;
  latestBlockNumber: number;
}

export interface ImportResult<TTarget> {
  target: TTarget;
  blockRange: Range<number>;
  latestBlockNumber: number;
  success: boolean;
}

export type ErrorEmitter<TObj, TQuery extends ImportQuery<TObj> = ImportQuery<TObj>> = (importQuery: TQuery) => void;
