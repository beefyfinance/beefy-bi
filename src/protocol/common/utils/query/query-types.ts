import { Range, SupportedRangeTypes } from "../../../../utils/range";

interface OptimizerOptions {
  ignoreImportState: boolean;
  maxAddressesPerQuery: number;
  maxRangeSize: number;
  maxQueriesPerProduct: number;
}

export interface OptimizerInput<TObj, TRange extends SupportedRangeTypes> {
  objKey: (obj: TObj) => string;
  states: {
    obj: TObj;
    fullRange: Range<TRange>;
    coveredRanges: Range<TRange>[];
    toRetry: Range<TRange>[];
  }[];
  options: OptimizerOptions;
}

export type RangeIndexBuilder<TObj, TRange extends SupportedRangeTypes> = (
  rangesToQuery: { obj: TObj; ranges: Range<TRange>[] }[],
  options: OptimizerOptions,
) => Range<TRange>[];

export interface JsonRpcBatchOutput<TObj, TRange extends SupportedRangeTypes> {
  type: "jsonrpc batch";
  obj: TObj;
  range: Range<TRange>;
}

export interface AddressBatchOutput<TObj, TRange extends SupportedRangeTypes> {
  type: "address batch";
  objs: TObj[];
  range: Range<TRange>;
  // filter events after the query since we allow bigger ranges than necessary
  postFilters: {
    obj: TObj;
    filter: Range<TRange>[] | "no-filter";
  }[];
}
export type QueryOptimizerOutput<TObj, TRange extends SupportedRangeTypes> = JsonRpcBatchOutput<TObj, TRange> | AddressBatchOutput<TObj, TRange>;

export interface StrategyInput<TObj, TRange extends SupportedRangeTypes> {
  objKey: (obj: TObj) => string;
  states: {
    obj: TObj;
    ranges: Range<TRange>[];
  }[];
  options: OptimizerOptions;
}

// anything internal and not exposed
export type StrategyResult<T> = {
  result: T[];
  totalCoverage: number;
  queryCount: number;
};
