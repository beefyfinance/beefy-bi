import { blockNumberListToRanges, createOptimizerIndexFromState, extractObjsAndRangeFromOptimizerOutput } from "./optimizer-utils";

describe("Optimizer utils", () => {
  it("should be able to identify distinct blobs and align queries with them", () => {
    expect(
      createOptimizerIndexFromState(
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
      createOptimizerIndexFromState(
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

    expect(
      createOptimizerIndexFromState(
        [
          [
            { from: 301, to: 500 },
            { from: 212, to: 244 },
            { from: 200, to: 209 },
          ],
        ],
        { mergeIfCloserThan: 1, verticalSlicesSize: 100 },
      ),
    ).toEqual([
      { from: 200, to: 209 },
      { from: 212, to: 244 },
      { from: 301, to: 400 },
      { from: 401, to: 500 },
    ]);
  });

  it("should be able to extract product ranges from an output", () => {
    expect(
      extractObjsAndRangeFromOptimizerOutput({
        objKey: (o) => o.key,
        output: { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 501, to: 899 } },
      }),
    ).toEqual([{ obj: { key: "0xA" }, range: { from: 501, to: 899 } }]);

    expect(
      extractObjsAndRangeFromOptimizerOutput({
        objKey: (o) => o.key,
        output: {
          type: "address batch",
          objs: [{ key: "0xA" }, { key: "0xB" }],
          range: { from: 1, to: 500 },
          postFilters: [
            {
              obj: { key: "0xA" },
              filter: [
                { from: 1, to: 99 },
                { from: 201, to: 500 },
              ],
            },
            {
              obj: { key: "0xB" },
              filter: [
                { from: 1, to: 99 },
                { from: 201, to: 399 },
              ],
            },
          ],
        },
      }),
    ).toEqual([
      { obj: { key: "0xA" }, range: { from: 1, to: 99 } },
      { obj: { key: "0xA" }, range: { from: 201, to: 500 } },
      { obj: { key: "0xB" }, range: { from: 1, to: 99 } },
      { obj: { key: "0xB" }, range: { from: 201, to: 399 } },
    ]);
    expect(
      extractObjsAndRangeFromOptimizerOutput({
        objKey: (o) => o.key,
        output: {
          type: "address batch",
          objs: [{ key: "0xA" }, { key: "0xB" }],
          range: { from: 100, to: 200 },
          postFilters: [
            {
              obj: { key: "0xA" },
              filter: "no-filter",
            },
            {
              obj: { key: "0xB" },
              filter: "no-filter",
            },
          ],
        },
      }),
    ).toEqual([
      { obj: { key: "0xA" }, range: { from: 100, to: 200 } },
      { obj: { key: "0xB" }, range: { from: 100, to: 200 } },
    ]);
  });
});

describe("blockNumberListToRanges", () => {
  it("should split a block number list into ranges, respecting the maxBlocksPerQuery parameter", () => {
    expect(
      blockNumberListToRanges({
        mode: "historical",
        latestBlockNumber: 100,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 30,
        msPerBlockEstimate: 1000,
        blockNumberList: [1, 10],
      }),
    ).toEqual([
      { from: 1, to: 9 },
      { from: 11, to: 40 },
      { from: 41, to: 70 },
      { from: 71, to: 100 },
    ]);
  });

  it("should respect the input block number list more than the snapshot interval", () => {
    expect(
      blockNumberListToRanges({
        mode: "historical",
        latestBlockNumber: 100,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 30,
        msPerBlockEstimate: 1000,
        blockNumberList: [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
      }),
    ).toEqual([
      { from: 1, to: 9 },
      { from: 10, to: 19 },
      { from: 20, to: 29 },
      { from: 30, to: 39 },
      { from: 40, to: 49 },
      { from: 50, to: 59 },
      { from: 60, to: 69 },
      { from: 70, to: 79 },
      { from: 80, to: 89 },
      { from: 90, to: 99 },
    ]);
  });

  it("should split a block number list into ranges, respecting the snapshotInterval and msPerBlockEstimate parameter", () => {
    expect(
      blockNumberListToRanges({
        mode: "historical",
        latestBlockNumber: 50,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 300_000,
        msPerBlockEstimate: 60_000,
        blockNumberList: [1, 10],
      }),
    ).toEqual([
      { from: 1, to: 9 },
      { from: 11, to: 25 },
      { from: 26, to: 40 },
      { from: 41, to: 50 },
    ]);
  });

  it("should split a block number list into ranges, and return only the most recent in recent mode", () => {
    expect(
      blockNumberListToRanges({
        mode: "recent",
        latestBlockNumber: 100,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 30,
        msPerBlockEstimate: 1000,
        blockNumberList: [1, 10],
      }),
    ).toEqual([{ from: 71, to: 100 }]);
  });
});
