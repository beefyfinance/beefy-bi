import { createOptimizerIndexFromBlockList } from "./optimizer-index-from-block-list";

describe("optimizer create index from block list", () => {
  it("should split a block number list into ranges, respecting the maxBlocksPerQuery parameter", () => {
    expect(
      createOptimizerIndexFromBlockList({
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
      createOptimizerIndexFromBlockList({
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
      createOptimizerIndexFromBlockList({
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
      createOptimizerIndexFromBlockList({
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
