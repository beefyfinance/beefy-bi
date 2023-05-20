import { optimizeSnapshotQueries } from "./optimize-snapshot-queries";

describe("snapshot optimizer", () => {
  it("should merge ranges when possible", () => {
    expect(
      optimizeSnapshotQueries({
        objKey: (obj) => obj.key,

        states: [
          { obj: { key: "0xA" }, fullRange: { from: 200, to: 500 }, coveredRanges: [], toRetry: [] },
          { obj: { key: "0xB" }, fullRange: { from: 300, to: 700 }, coveredRanges: [], toRetry: [] },
          { obj: { key: "0xC" }, fullRange: { from: 480, to: 630 }, coveredRanges: [], toRetry: [] },
        ],
        options: {
          ignoreImportState: false,
          maxAddressesPerQuery: 1000,
          maxRangeTimeStep: "15min",
          maxQueriesPerProduct: 1000,
        },
      }),
    ).toEqual([
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }, { key: "0xC" }],
        range: { from: 200, to: 700 },
        postFilters: [
          { obj: { key: "0xA" }, filter: [{ from: 200, to: 500 }] },
          { obj: { key: "0xB" }, filter: [{ from: 300, to: 700 }] },
          { obj: { key: "0xC" }, filter: [{ from: 480, to: 630 }] },
        ],
      },
    ]);
  });
});
