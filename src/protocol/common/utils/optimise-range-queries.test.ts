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
          productKeys: ["0xA", "0xB", "0xC"],
          range: { from: 200, to: 700 },
          postFilters: [
            { productKey: "0xA", ranges: [{ from: 200, to: 500 }] },
            { productKey: "0xB", ranges: [{ from: 300, to: 700 }] },
            { productKey: "0xC", ranges: [{ from: 480, to: 630 }] },
          ],
        },
      ],
    });
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
        100,
      ),
    ).toEqual([
      { from: 200, to: 700 },
      { from: 1333, to: 1350 },
    ]);
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
        {
          productKey: "0xA",
          range: { from: 100, to: 200 },
        },
        {
          productKey: "0xB",
          range: { from: 200, to: 300 },
        },
        {
          productKey: "0xC",
          range: { from: 300, to: 400 },
        },
      ],
    });
  });
});
