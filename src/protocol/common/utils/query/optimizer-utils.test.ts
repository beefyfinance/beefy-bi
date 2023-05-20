import { RpcConfig } from "../../../../types/rpc-config";
import { LogInfos } from "../../../../utils/logger";
import { defaultLimitations } from "../../../../utils/rpc/rpc-limitations";
import { ImportBehaviour, defaultImportBehaviour } from "../../types/import-context";
import { _buildRangeIndex, extractObjsAndRangeFromOptimizerOutput, importStateToOptimizerRangeInput } from "./optimizer-utils";

describe("Optimizer utils", () => {
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

    expect(
      _buildRangeIndex(
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

describe("Prepare the import state for the optimizer based on current configuration: mode = recent", () => {
  const logInfos: LogInfos = { msg: "test" };
  const rpcConfig: RpcConfig = {
    batchProvider: null as any,
    linearProvider: null as any,
    chain: "bsc",
    rpcLimitations: {
      ...defaultLimitations,
      maxGetLogsBlockSpan: 10,
    },
  };
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
        logInfos,
        rpcConfig,
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
        logInfos,
        rpcConfig,
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100_000 } },
        importState: null,
      }),
    ).toEqual({ fullRange: { from: 100_000_000 - 23801 - 5, to: 100_000_000 - 5 }, coveredRanges: [], toRetry: [] });
  });

  it("should exclude the retry ranges ", () => {
    expect(
      importStateToOptimizerRangeInput({
        behaviour: { ...behaviour, waitForBlockPropagation: 30 },
        isLive: true,
        lastImportedBlockNumber: null,
        latestBlockNumber: 200,
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
  const logInfos: LogInfos = { msg: "test" };
  const rpcConfig: RpcConfig = {
    batchProvider: null as any,
    linearProvider: null as any,
    chain: "bsc",
    rpcLimitations: {
      ...defaultLimitations,
      maxGetLogsBlockSpan: 10,
    },
  };
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
        logInfos,
        rpcConfig,
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
        logInfos,
        rpcConfig,
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100_000 } },
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
        logInfos,
        rpcConfig: { ...rpcConfig, rpcLimitations: { ...rpcConfig.rpcLimitations, maxGetLogsBlockSpan: 100 } },
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
