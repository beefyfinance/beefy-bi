import { groupBy, keyBy, max, sum } from "lodash";
import { ProgrammerError } from "../../../utils/programmer-error";
import { Range, SupportedRangeTypes, getRangeSize, rangeSortedArrayExclude, rangeSortedSplitManyToMaxLengthAndTakeSome } from "../../../utils/range";

interface Input<TRange extends SupportedRangeTypes> {
  states: {
    productKey: string;
    fullRange: Range<TRange>;
    coveredRanges: Range<TRange>[];
    toRetry: Range<TRange>[];
  }[];
  options: {
    ignoreImportState: boolean;
    maxAddresses: number;
    maxRangeSize: number;
    maxQueries: number;
  };
}

interface JsonRpcBatchOutput<TRange extends SupportedRangeTypes> {
  type: "jsonrpc batch";
  queries: {
    productKey: string;
    range: Range<TRange>;
  }[];
}

interface AddressBatchOutput<TRange extends SupportedRangeTypes> {
  type: "address batch";
  queries: {
    productKeys: string[];
    range: Range<TRange>;
    // filter events after the query since we allow bigger ranges than necessary
    postFilters: {
      productKey: string;
      ranges: Range<TRange>[];
    }[];
  }[];
}

// anything internal and not exposed
type StrategyResult<T> = {
  result: T;
  totalCoverage: number;
  queryCount: number;
};

type Output<TRange extends SupportedRangeTypes> = JsonRpcBatchOutput<TRange> | AddressBatchOutput<TRange>;

export function optimiseRangeQueries<TRange extends SupportedRangeTypes>(input: Input<TRange>): Output<TRange> {
  // ensure we only have one input state per product
  const statesByProduct = groupBy(input.states, (s) => s.productKey);
  const duplicateStatesByProduct = Object.values(statesByProduct).filter((states) => states.length > 1);
  if (duplicateStatesByProduct.length > 0) {
    throw new ProgrammerError({ msg: "Duplicate states by product", data: { duplicateStatesByProduct } });
  }

  const jsonRpcBatch = optimizeForJsonRpcBatch(input);
  const addressBatch = optimizeForAddressBatch(input);

  let output: StrategyResult<Output<TRange>>;
  // use jsonrpc batch if there is a tie in request count, which can happen when we have a low maxQueries option
  // we want to use the method with the most coverage
  if (jsonRpcBatch.queryCount === addressBatch.queryCount) {
    output = jsonRpcBatch.totalCoverage > addressBatch.totalCoverage ? jsonRpcBatch : addressBatch;
  } else {
    // otherwise use the method with the least queries
    output = jsonRpcBatch.queryCount < addressBatch.queryCount ? jsonRpcBatch : addressBatch;
  }

  return output.result;
}

function optimizeForJsonRpcBatch<TRange extends SupportedRangeTypes>(input: Input<TRange>): StrategyResult<Output<TRange>> {
  // first, we can only work one
  /*
  const queriesByAddress = groupBy(inputQueries, (q) => q.address);
  Object.entries(queriesByAddress).flatMap(([address, queries]) => ({
    address,
    range: rangeSortedSplitManyToMaxLengthAndTakeSome(rangeMerge(queries.map((q) => q.range))),
  }));
  for (const [address, addressQueries] of Object.entries(queriesByAddress)) {
    addressQueries.map((q) => q.range);
  }
*/
  return {
    result: {
      type: "jsonrpc batch",
      queries: [],
    },
    totalCoverage: 0,
    queryCount: 1000,
  };
}

function optimizeForAddressBatch<TRange extends SupportedRangeTypes>({
  states,
  options: { ignoreImportState, maxQueries, maxRangeSize },
}: Input<TRange>): StrategyResult<Output<TRange>> {
  const queries = states.flatMap(({ productKey, fullRange, coveredRanges, toRetry }) => {
    let ranges = [fullRange];

    // exclude covered ranges and retry ranges
    if (!ignoreImportState) {
      ranges = rangeSortedArrayExclude([fullRange], [...coveredRanges, ...toRetry]);
    }

    // split in ranges no greater than the maximum allowed
    // order by new range first since it's more important and more likely to be available via RPC calls
    ranges = rangeSortedSplitManyToMaxLengthAndTakeSome(ranges, maxRangeSize, maxQueries, "desc");

    // if there is room, add the ranges that failed to be imported
    if (ranges.length < maxQueries) {
      toRetry = rangeSortedSplitManyToMaxLengthAndTakeSome(toRetry, maxRangeSize, maxQueries - ranges.length, "desc");

      // put retries last
      ranges = ranges.concat(toRetry);
    }
    // limit the amount of queries sent
    if (ranges.length > maxQueries) {
      ranges = ranges.slice(0, maxQueries);
    }

    return ranges.map((range) => ({ productKey, range }));
  });

  const totalCoverage = sum(queries.map((q) => getRangeSize(q.range)));
  return {
    result: {
      type: "jsonrpc batch",
      queries: queries,
    },
    totalCoverage,
    queryCount: queries.length,
  };
}
