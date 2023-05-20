import { chunk, groupBy, max, min, sortBy, sum } from "lodash";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import {
  Range,
  SupportedRangeTypes,
  getRangeSize,
  isValidRange,
  rangeCovering,
  rangeEqual,
  rangeIntersect,
  rangeMerge,
  rangeSortedArrayExclude,
  rangeSortedSplitManyToMaxLengthAndTakeSome,
} from "../../../../utils/range";
import { _buildRangeIndex, getLoggableInput, getLoggableOptimizerOutput } from "./optimizer-utils";
import {
  AddressBatchOutput,
  JsonRpcBatchOutput,
  QueryOptimizerOutput,
  RangeQueryOptimizerInput,
  RangeQueryOptimizerOptions,
  StrategyInput,
  StrategyResult,
} from "./query-types";

const logger = rootLogger.child({ module: "common", component: "optimise-range-queries" });

/**
 * Find a good way to batch queries to minimize the quey count while also respecting the following constraints:
 * - maxAddressesPerQuery: some rpc can't accept too much addresses in their batch, or they will timeout if there is too much data
 * - maxRangeSize: most rpc restrict on how much range we can query
 * - maxQueriesPerProduct: mostly a way to avoid consuming too much memory
 *
 */
export function optimizeRangeQueries<TObj, TRange extends SupportedRangeTypes>(
  input: RangeQueryOptimizerInput<TObj, TRange>,
): QueryOptimizerOutput<TObj, TRange>[] {
  logger.debug({ msg: "Optimising a range queries", data: getLoggableInput(input) });

  const {
    states,
    options: { ignoreImportState, maxRangeSize, maxQueriesPerProduct },
  } = input;

  // ensure we only have one input state per product
  const statesByProduct = groupBy(states, (s) => input.objKey(s.obj));
  const duplicateStatesByProduct = Object.values(statesByProduct).filter((states) => states.length > 1);
  if (duplicateStatesByProduct.length > 0) {
    throw new ProgrammerError({ msg: "Duplicate states by product", data: { duplicateStatesByProduct } });
  }

  // check all input ranges
  const isRangeInvalid = (r: Range<TRange>) => {
    const result = !isValidRange(r);
    if (result) {
      logger.error({ msg: "Invalid range found", data: r });
    }
    return result;
  };
  const hasSomeInvalidRange = states.some(
    (state) => isRangeInvalid(state.fullRange) || state.coveredRanges.some((r) => isRangeInvalid(r)) || state.toRetry.some((r) => isRangeInvalid(r)),
  );
  if (hasSomeInvalidRange) {
    logger.error({ msg: "Invalid range found in input", data: getLoggableInput(input) });
    return [];
  }

  // if we need to retry some, do it at the end
  const steps = ignoreImportState
    ? [states.map(({ obj, fullRange }) => ({ obj, ranges: [fullRange] }))]
    : [
        // put retries at the end
        states.map(({ obj, fullRange, coveredRanges, toRetry }) => ({
          obj,
          ranges: rangeSortedArrayExclude([fullRange], [...coveredRanges, ...toRetry]).reverse(),
        })),
        states.map(({ obj, toRetry }) => ({ obj, ranges: toRetry.reverse() })),
      ];

  const bestStrategiesBySlice = steps.flatMap((rangesToQuery) =>
    // build the coverage index for non-retry ranges
    _buildRangeIndex(
      rangesToQuery.map((s) => s.ranges),
      { mergeIfCloserThan: Math.round(maxRangeSize / 2), verticalSlicesSize: maxRangeSize },
    )
      // handle the most recent first
      .reverse()
      // limit the amount we need to fetch
      .slice(0, maxQueriesPerProduct)
      // restrict ranges by the index
      .map((rangeIndexPart) =>
        rangesToQuery.map(({ obj, ranges }) => ({ obj, ranges: rangeIntersect(ranges, rangeIndexPart) })).filter(({ ranges }) => ranges.length > 0),
      )
      // get the best method for each part of this index
      .flatMap((toQuery) => _findTheBestMethodForRanges<TObj, TRange>({ objKey: input.objKey, states: toQuery, options: input.options })),
  );

  logger.trace({
    msg: "Best strategy for all slice found",
    data: { input: getLoggableInput(input), bestStrategiesBySlice: getLoggableOptimizerOutput(input, bestStrategiesBySlice) },
  });

  return bestStrategiesBySlice;
}

/**
 * For a given indexed range, find the best method
 */
function _findTheBestMethodForRanges<TObj, TRange extends SupportedRangeTypes>(
  input: StrategyInput<TObj, RangeQueryOptimizerOptions, TRange>,
): QueryOptimizerOutput<TObj, TRange>[] {
  // sometimes we just can't batch by address
  if (input.options.maxAddressesPerQuery === 1) {
    return _getJsonRpcBatchQueries<TObj, TRange>(input).result;
  }

  const jsonRpcBatch = _getJsonRpcBatchQueries(input);
  const addressBatch = _getAddressBatchQueries(input);

  let output: StrategyResult<QueryOptimizerOutput<TObj, TRange>>;
  // use jsonrpc batch if there is a tie in request count, which can happen when we have a low maxQueries option
  // we want to use the method with the most coverage
  if (jsonRpcBatch.queryCount === addressBatch.queryCount) {
    // if the coverage is the same
    if (jsonRpcBatch.totalCoverage === addressBatch.totalCoverage) {
      // and only one product key in the address batch no need to do address batching
      const maxProductKeyCount = max(addressBatch.result.map((q) => q.objs.length)) || 1;
      output = maxProductKeyCount <= 1 ? jsonRpcBatch : addressBatch;
    } else {
      output = jsonRpcBatch.totalCoverage > addressBatch.totalCoverage ? jsonRpcBatch : addressBatch;
    }
  } else {
    // otherwise use the method with the least queries
    output = jsonRpcBatch.queryCount < addressBatch.queryCount ? jsonRpcBatch : addressBatch;
  }

  logger.trace({ msg: "Best strategy for slice found", data: { input: getLoggableInput(input), output: getLoggableOptimizerOutput(input, output) } });
  return output.result;
}

/**
 * Do some batching using this probably not optimal algorithm
 *
 * Given the data needs below:
 *
 * 0x1: [100,299]
 * 0x2: [200,399]
 * 0x3: [250,349]
 *
 * Now, we pick a cell size, something like x% of the max size. X being a configuration of this algorithm.
 *
 * Say we got a cell size of 50 from now on, a maxRangeSize of 100 and maxAddressesPerQuery of 2.
 * We place the product queries in a grid like so:
 *
 *     | [100,149] [150,199] [200,249] [250,299] [300,349] [350,399] |
 * ----------------------------------------------------------------- |
 * 0x1 |     x         x         x         x                         |
 * 0x2 |                         x         x         x         x     |
 * 0x3 |                                   x         x               |
 * ----------------------------------------------------------------- |
 *
 * The individual cells represent data needs, please note that it's not a regular grid in the sense that rows are not ordered.
 *
 * Now we try to "fill" this grid using rectangles of length `maxRangeSize` and height `maxAddressesPerQuery`. Those will be our queries.
 * To create a query, we start from the left-most data-need and include the whole `maxRangeSize` of this product.
 * Until we have filled the `maxAddressesPerQuery` requirement, find the product that would benefit the most from being added to the batch and add it.
 * Repeat until all the grid is covered.
 *
 * For the previous example, the result would be this:
 *
 *     | [100,149] [150,199] [200,249] [250,299] [300,349] [350,399] |
 * ----------------------------------------------------------------- |
 * 0x1 | [1  x         x  1] [2   x        x  2]                     |
 * 0x2 |                     [2   x        x  2] [4  x         x  4] |
 * 0x3 |                     [3            x  3] [4  x            4] |
 * ----------------------------------------------------------------- |
 *
 * A special case that will happen often is when there is recent data to cover and very old data to reimport.
 * In this case we have a grid with data blobs separated by large gaps we don't want to look at for performance.
 *
 * To handle those cases we build a range index composed of every cell span.
 * Example with 0x4 [150,199] [550,599] below:
 *
 *     | [100,149] [150,199] [200,249] [250,299] [300,349] [350,399] [400,449] [450,499] [500,549] [550,599] |
 * --------------------------------------------------------------------------------------------------------- |
 * 0x1 |     x         x         x         x                                                                 |
 * 0x2 |                         x         x         x         x                                             |
 * 0x3 |                                   x         x                                                       |
 * 0x4 |               x                                                                     x               |
 * --------------------------------------------------------------------------------------------------------- |
 * => Range index: [[100, 399], [500, 549]].
 *
 * This way we can realign the range queries each time and have a better coverage for every blob of data.
 * Sometimes the blobs are close enough that we can merge them
 *
 * PS: note that the implementation could be way faster using a grid of bits and bitwise operations.
 */
function _getAddressBatchQueries<TObj, TRange extends SupportedRangeTypes>({
  objKey,
  states,
  options: { maxAddressesPerQuery, maxQueriesPerProduct, maxRangeSize },
}: StrategyInput<TObj, RangeQueryOptimizerOptions, TRange>): StrategyResult<AddressBatchOutput<TObj, TRange>> {
  let toQuery = states.map(({ obj, ranges }) => ({
    obj,
    ranges,
    min: min(ranges.map((r) => r.from)) as number,
    max: max(ranges.map((r) => r.to)) as number,
    coverage: sum(ranges.map((r) => getRangeSize(r))),
    random: Math.random(),
  }));

  // now we build queries
  // find the product that would benefit the most from being included by sorting by range size, use random in case of tie
  toQuery = sortBy(
    toQuery,
    (s) => s.coverage,
    (s) => s.random,
  ).reverse();

  let queries = chunk(toQuery, maxAddressesPerQuery).map((parts) => {
    const coveringRange = rangeCovering(parts.flatMap((part) => part.ranges));
    return {
      // sort to make tests reliable
      objs: sortBy(
        parts.map((part) => part.obj),
        objKey,
      ),
      range: coveringRange,
      postFilters: sortBy(
        parts
          .map((part) => ({
            obj: part.obj,
            ranges: rangeMerge(part.ranges).filter((r) => !rangeEqual(r, coveringRange)),
          }))
          .map(({ obj, ranges }) => ({ obj, filter: ranges.length > 0 ? ranges : ("no-filter" as const) })),
        (i) => objKey(i.obj),
      ),
      coverage: sum(parts.flatMap((part) => part.ranges).map((r) => getRangeSize(r))),
    };
  });

  return {
    result: queries.map(({ objs, range, postFilters }) => ({
      type: "address batch",
      objs,
      range,
      postFilters,
    })),
    totalCoverage: sum(queries.map((q) => q.coverage)),
    queryCount: queries.length,
  };
}

/**
 * A simple method where we simply do one request per product range
 */
function _getJsonRpcBatchQueries<TObj, TRange extends SupportedRangeTypes>({
  states,
  options: { maxQueriesPerProduct, maxRangeSize },
}: StrategyInput<TObj, RangeQueryOptimizerOptions, TRange>): StrategyResult<JsonRpcBatchOutput<TObj, TRange>> {
  const queries = states.flatMap(({ obj, ranges }) => {
    // split in ranges no greater than the maximum allowed
    // order by new range first since it's more important and more likely to be available via RPC calls
    ranges = rangeSortedSplitManyToMaxLengthAndTakeSome(ranges, maxRangeSize, maxQueriesPerProduct, "desc");

    // limit the amount of queries sent
    if (ranges.length > maxQueriesPerProduct) {
      ranges = ranges.slice(0, maxQueriesPerProduct);
    }

    return ranges.map((range) => ({ obj, range }));
  });

  const totalCoverage = sum(queries.map((q) => getRangeSize(q.range)));
  return {
    result: queries.map((q) => ({ type: "jsonrpc batch", ...q })),
    totalCoverage,
    queryCount: queries.length,
  };
}
