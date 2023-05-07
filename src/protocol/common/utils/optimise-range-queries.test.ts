import deepFreeze from "deep-freeze";
import { _buildRangeIndex, optimiseRangeQueries } from "./optimise-range-queries";

describe("range aggregator", () => {
  it("should merge ranges when possible", () => {
    expect(
      optimiseRangeQueries({
        states: [
          { productKey: "0xA", fullRange: { from: 200, to: 500 }, coveredRanges: [], toRetry: [] },
          { productKey: "0xB", fullRange: { from: 300, to: 700 }, coveredRanges: [], toRetry: [] },
          { productKey: "0xC", fullRange: { from: 480, to: 630 }, coveredRanges: [], toRetry: [] },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1000,
          maxRangeSize: 1000,
          maxQueriesPerProduct: 1000,
        },
      }),
    ).toEqual({
      type: "address batch",
      queries: [
        {
          productKeys: ["0xB", "0xA", "0xC"],
          range: { from: 200, to: 700 },
          postFilters: [
            { productKey: "0xB", ranges: [{ from: 300, to: 700 }] },
            { productKey: "0xA", ranges: [{ from: 200, to: 500 }] },
            { productKey: "0xC", ranges: [{ from: 480, to: 630 }] },
          ],
        },
      ],
    });
  });

  it("should not merge ranges when it doesn't make sense", () => {
    expect(
      optimiseRangeQueries({
        states: [
          { productKey: "0xA", fullRange: { from: 100, to: 200 }, coveredRanges: [], toRetry: [] },
          { productKey: "0xB", fullRange: { from: 200, to: 300 }, coveredRanges: [], toRetry: [] },
          { productKey: "0xC", fullRange: { from: 300, to: 400 }, coveredRanges: [], toRetry: [] },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1,
          maxRangeSize: 1000,
          maxQueriesPerProduct: 1000,
        },
      }),
    ).toEqual({
      type: "jsonrpc batch",
      queries: [
        { productKey: "0xA", range: { from: 100, to: 200 } },
        { productKey: "0xB", range: { from: 200, to: 300 } },
        { productKey: "0xC", range: { from: 300, to: 400 } },
      ],
    });
  });

  it("should exclude ranges that are already covered if asked so", () => {
    expect(
      optimiseRangeQueries({
        states: [
          { productKey: "0xA", fullRange: { from: 200, to: 500 }, coveredRanges: [{ from: 245, to: 300 }], toRetry: [{ from: 210, to: 211 }] },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1,
          maxRangeSize: 100,
          maxQueriesPerProduct: 1000,
        },
      }),
    ).toEqual({
      type: "jsonrpc batch",
      queries: [
        // first, the most recent blocks
        { productKey: "0xA", range: { from: 401, to: 500 } },
        { productKey: "0xA", range: { from: 301, to: 400 } },
        { productKey: "0xA", range: { from: 212, to: 244 } },
        { productKey: "0xA", range: { from: 200, to: 209 } },
        // after that, the retries
        { productKey: "0xA", range: { from: 210, to: 211 } },
      ],
    });
  });

  it("should respect the maxRangeSize parameter", () => {
    expect(
      optimiseRangeQueries({
        states: [
          { productKey: "0xA", fullRange: { from: 1, to: 1000 }, coveredRanges: [{ from: 900, to: 1000 }], toRetry: [{ from: 901, to: 909 }] },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1,
          maxRangeSize: 10,
          maxQueriesPerProduct: 10,
        },
      }),
    ).toEqual({
      type: "jsonrpc batch",
      queries: [
        { productKey: "0xA", range: { from: 890, to: 899 } },
        { productKey: "0xA", range: { from: 880, to: 889 } },
        { productKey: "0xA", range: { from: 870, to: 879 } },
        { productKey: "0xA", range: { from: 860, to: 869 } },
        { productKey: "0xA", range: { from: 850, to: 859 } },
        { productKey: "0xA", range: { from: 840, to: 849 } },
        { productKey: "0xA", range: { from: 830, to: 839 } },
        { productKey: "0xA", range: { from: 820, to: 829 } },
        { productKey: "0xA", range: { from: 810, to: 819 } },
        { productKey: "0xA", range: { from: 800, to: 809 } },
      ],
    });
  });

  it("should respect the maxQueriesPerProduct parameter", async () => {
    expect(
      optimiseRangeQueries({
        states: [
          { productKey: "0xA", fullRange: { from: 1, to: 1000 }, coveredRanges: [{ from: 900, to: 1000 }], toRetry: [{ from: 901, to: 909 }] },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1,
          maxRangeSize: 500,
          maxQueriesPerProduct: 3,
        },
      }),
    ).toEqual({
      type: "jsonrpc batch",
      queries: [
        { productKey: "0xA", range: { from: 400, to: 899 } },
        { productKey: "0xA", range: { from: 1, to: 399 } },
        { productKey: "0xA", range: { from: 901, to: 909 } },
      ],
    });
  });

  it("should merge input ranges when possible", async () => {
    expect(
      optimiseRangeQueries({
        states: [
          {
            productKey: "0xA",
            fullRange: { from: 1, to: 1000 },
            coveredRanges: [
              { from: 900, to: 950 },
              { from: 920, to: 1000 },
            ],
            toRetry: [{ from: 901, to: 909 }],
          },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1,
          maxRangeSize: 500,
          maxQueriesPerProduct: 3,
        },
      }),
    ).toEqual({
      type: "jsonrpc batch",
      queries: [
        { productKey: "0xA", range: { from: 400, to: 899 } },
        { productKey: "0xA", range: { from: 1, to: 399 } },
        { productKey: "0xA", range: { from: 901, to: 909 } },
      ],
    });
  });

  it("should not update any input object", async () => {
    const input = deepFreeze({
      states: [
        {
          productKey: "0xA",
          fullRange: { from: 1, to: 1000 },
          coveredRanges: [
            { from: 900, to: 950 },
            { from: 920, to: 1000 },
          ],
          toRetry: [{ from: 901, to: 909 }],
        },
      ],
      options: {
        ignoreImportState: false,
        maxAddressesPerQuery: 1,
        maxRangeSize: 500,
        maxQueriesPerProduct: 3,
      },
    });

    optimiseRangeQueries(input as Parameters<typeof optimiseRangeQueries>[0]);
  });

  it("should be able to identify distinct blobs and align queries with them", () => {
    expect(
      _buildRangeIndex(
        [
          [
            { from: 200, to: 500 },
            { from: 300, to: 700 },
          ],
          [
            { from: 480, to: 630 },
            { from: 1333, to: 1350 },
          ],
        ],
        { mergeIfCloserThan: 100, verticalSlicesSize: 1000 },
      ),
    ).toEqual([
      { from: 200, to: 700 },
      { from: 1333, to: 1350 },
    ]);

    expect(
      _buildRangeIndex(
        [
          [
            { from: 200, to: 500 },
            { from: 300, to: 700 },
          ],
          [
            { from: 480, to: 630 },
            { from: 1333, to: 1350 },
          ],
        ],
        { mergeIfCloserThan: 100, verticalSlicesSize: 200 },
      ),
    ).toEqual([
      { from: 200, to: 399 },
      { from: 400, to: 599 },
      { from: 600, to: 700 },
      { from: 1333, to: 1350 },
    ]);
  });
});
