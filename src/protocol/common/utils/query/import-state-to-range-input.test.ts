import { ImportBehaviour, defaultImportBehaviour } from "../../types/import-context";
import { importStateToOptimizerRangeInput } from "./import-state-to-range-input";

describe("Prepare the import state for the optimizer based on current configuration: mode = recent", () => {
  const behaviour: ImportBehaviour = {
    ...defaultImportBehaviour,
    mode: "recent",
    waitForBlockPropagation: 5,
  };

  it("should consider the latest range only", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour,
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 100,
        maxBlocksPerQuery: 10,
        msPerBlockEstimate: 1000,
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 85, to: 95 }, coveredRanges: [], toRetry: [] });
  });

  it("should account for waitForBlockPropagation", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 10,
        msPerBlockEstimate: 1000,
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 160, to: 170 }, coveredRanges: [], toRetry: [] });
  });

  it("should ignore a non null and empty emport state", () => {
    // with a fresh import state and a different rpc config
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [],
              toRetry: [],
            },
          } as any,
        },
      }),
    ).toEqual({ fullRange: { from: 70, to: 170 }, coveredRanges: [], toRetry: [] });
  });

  it("should use the lastImportedBlockNumber as the starting point when provided", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour,
        isLive: true,
        lastImportedBlockNumber: 90,
        latestBlockNumber: 100,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 91, to: 95 }, coveredRanges: [], toRetry: [] });
  });

  it("should not be more than 1day of block ranges", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour,
        isLive: true,
        lastImportedBlockNumber: 100_000,
        latestBlockNumber: 100_000_000,
        maxBlocksPerQuery: 100_000,
        msPerBlockEstimate: 1000,
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 100_000_000 - 86400 - 5, to: 100_000_000 - 5 }, coveredRanges: [], toRetry: [] });
  });

  it("should exclude the retry ranges ", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [
                { from: 1, to: 10 },
                { from: 100, to: 150 },
              ],
              toRetry: [
                { from: 50, to: 60 },
                { from: 160, to: 170 },
              ],
            },
          } as any,
        },
      }),
    ).toEqual({
      fullRange: { from: 70, to: 170 },
      coveredRanges: [
        { from: 1, to: 10 },
        { from: 100, to: 150 },
      ],
      // ignore retries
      toRetry: [],
    });
  });
});

describe("Prepare the import state for the optimizer based on current configuration: mode = historical", () => {
  const behaviour: ImportBehaviour = {
    ...defaultImportBehaviour,
    mode: "historical",
    waitForBlockPropagation: 5,
  };

  it("should still return when there is no import state", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour,
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 100,
        maxBlocksPerQuery: 10,
        msPerBlockEstimate: 1000,
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 85, to: 95 }, coveredRanges: [], toRetry: [] });
  });

  it("should account for waitForBlockPropagation", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 10,
        msPerBlockEstimate: 1000,
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 160, to: 170 }, coveredRanges: [], toRetry: [] });
  });

  it("consider the full range starting from contract creation", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [],
              toRetry: [],
            },
          } as any,
        },
      }),
    ).toEqual({ fullRange: { from: 1, to: 170 }, coveredRanges: [], toRetry: [] });
  });

  it("should account for the forceConsideredBlockRange behaviour", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30, forceConsideredBlockRange: { from: 45, to: 75 } },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [],
              toRetry: [],
            },
          } as any,
        },
      }),
    ).toEqual({ fullRange: { from: 45, to: 75 }, coveredRanges: [], toRetry: [] });
  });

  it("should ignore the lastImportedBlockNumber as the starting point when provided", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour,
        isLive: true,
        lastImportedBlockNumber: 90,
        latestBlockNumber: 100,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [],
              toRetry: [],
            },
          } as any,
        },
      }),
    ).toEqual({ fullRange: { from: 1, to: 95 }, coveredRanges: [], toRetry: [] });
  });

  it("should not be more than 1day of block ranges at once", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour,
        isLive: true,
        lastImportedBlockNumber: 100_000,
        latestBlockNumber: 100_000_000,
        maxBlocksPerQuery: 100_000,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [],
              toRetry: [],
            },
          } as any,
        },
      }),
    ).toEqual({ fullRange: { from: 1, to: 100_000_000 - 5 }, coveredRanges: [], toRetry: [] });
  });

  it("should consider the full range in historical mode", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, mode: "historical", waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        maxBlocksPerQuery: 100,
        msPerBlockEstimate: 1000,
        importState: {
          importKey: "test",
          importData: {
            contractCreatedAtBlock: 1,
            ranges: {
              coveredRanges: [
                { from: 1, to: 10 },
                { from: 100, to: 150 },
              ],
              toRetry: [
                { from: 50, to: 60 },
                { from: 160, to: 170 },
              ],
            },
          } as any,
        },
      }),
    ).toEqual({
      fullRange: { from: 1, to: 170 },
      coveredRanges: [
        { from: 1, to: 10 },
        { from: 100, to: 150 },
      ],
      toRetry: [
        { from: 50, to: 60 },
        { from: 160, to: 170 },
      ],
    });
  });
});
