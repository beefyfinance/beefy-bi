import { createOptimizerIndexFromBlockList } from "./optimizer-index-from-block-list";

describe("optimizer create index from block list", () => {
  it("should split a block number list into ranges, respecting the maxBlocksPerQuery parameter", () => {
    expect(
      createOptimizerIndexFromBlockList({
        mode: "historical",
        latestBlockNumber: 100,
        firstBlockToConsider: 1,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 30,
        msPerBlockEstimate: 1000,
        blockNumberList: [1, 10],
      }),
    ).toEqual([
      { from: 1, to: 9 },
      { from: 10, to: 39 },
      { from: 40, to: 69 },
      { from: 70, to: 99 },
      { from: 100, to: 100 },
    ]);
  });

  it("should respect the input block number list more than the snapshot interval", () => {
    expect(
      createOptimizerIndexFromBlockList({
        mode: "historical",
        latestBlockNumber: 100,
        firstBlockToConsider: 1,
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
        firstBlockToConsider: 1,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 300_000,
        msPerBlockEstimate: 60_000,
        blockNumberList: [1, 10],
      }),
    ).toEqual([
      { from: 1, to: 9 },
      { from: 10, to: 24 },
      { from: 25, to: 39 },
      { from: 40, to: 50 },
    ]);
  });

  it("should split a block number list into ranges, and return only the most recent in recent mode", () => {
    expect(
      createOptimizerIndexFromBlockList({
        mode: "recent",
        latestBlockNumber: 80,
        firstBlockToConsider: 1,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 30,
        msPerBlockEstimate: 1000,
        blockNumberList: [1, 10],
      }),
    ).toEqual([{ from: 70, to: 80 }]);
  });

  it("should extend the block list backward if that's necessary", () => {
    expect(
      createOptimizerIndexFromBlockList({
        mode: "historical",
        latestBlockNumber: 35,
        firstBlockToConsider: 1,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 10,
        msPerBlockEstimate: 1000,
        blockNumberList: [10, 20],
      }),
    ).toEqual([
      { from: 1, to: 9 },
      { from: 10, to: 19 },
      { from: 20, to: 29 },
      { from: 30, to: 35 },
    ]);
  });

  it("should not crash when the input parameters lead to an invalid range", () => {
    expect(
      createOptimizerIndexFromBlockList({
        mode: "historical",
        latestBlockNumber: 10,
        firstBlockToConsider: 1,
        snapshotInterval: "15min",
        maxBlocksPerQuery: 10,
        msPerBlockEstimate: 1000,
        blockNumberList: [10, 10],
      }),
    ).toEqual([{ from: 1, to: 9 }]);
  });
});
