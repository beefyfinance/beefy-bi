import { RpcLimitations } from "../../../utils/rpc/rpc-limitations";
import { getBatchConfigFromLimitations } from "./batch-rpc-calls";

describe("batchRpcCalls$", () => {
  it.each([
    ["eth_getLogs", 1, 100, true, 100],
    ["eth_blockNumber", 1, 100, true, 100],
    ["eth_getBlockByNumber", 1, 100, true, 100],
    ["eth_getTransactionReceipt", 1, 100, true, 100],
    ["eth_call", 1, 100, true, 100],
    ["eth_call", 2, 100, true, 50],
    ["eth_call", 3, 100, true, 33],
    ["eth_call", 3, 50, true, 16],
    ["eth_call", 1, 1, false, 1],
    ["eth_call", 1, 100, true, 100],
    ["eth_call", 1, null, false, 1],
  ] as [keyof RpcLimitations["methods"], number, number, boolean, number][])(
    "should compute batch params from limitations and method config (%s, %s, %s, %s, %s)",
    async (method, callPerInputObjs, methodLimitation, expectedCanUseBatch, expectedMaxInputPerBatch) => {
      const params = {
        maxInputObjsPerBatch: 10_000,
        logInfos: { msg: "test" },
        rpcCallsPerInputObj: {
          eth_getLogs: 0,
          eth_blockNumber: 0,
          eth_call: 0,
          eth_getBlockByNumber: 0,
          eth_getTransactionReceipt: 0,
        },
        limitations: {
          restrictToMode: null,
          isArchiveNode: true,
          disableBatching: false,
          disableRpc: false,
          weight: null,
          internalTimeoutMs: 10_000,
          maxGetLogsBlockSpan: 10_000,
          maxGetLogsAddressBatchSize: null,
          methods: {
            eth_blockNumber: null,
            eth_call: null,
            eth_getBlockByNumber: null,
            eth_getLogs: null,
            eth_getTransactionReceipt: null,
          },
          minDelayBetweenCalls: 0,
        } as RpcLimitations,
      };

      params.rpcCallsPerInputObj[method] = callPerInputObjs;
      params.limitations.methods[method] = methodLimitation;

      expect(getBatchConfigFromLimitations(params)).toEqual({
        canUseBatchProvider: expectedCanUseBatch,
        maxInputObjsPerBatch: expectedMaxInputPerBatch,
      });
    },
  );

  it("should batch to the minimum size when multiple methods are hit", () => {
    expect(
      getBatchConfigFromLimitations({
        maxInputObjsPerBatch: 10_000,
        logInfos: { msg: "test" },
        rpcCallsPerInputObj: {
          eth_getLogs: 0,
          eth_blockNumber: 10,
          eth_call: 0,
          eth_getBlockByNumber: 2,
          eth_getTransactionReceipt: 0,
        },
        limitations: {
          restrictToMode: null,
          isArchiveNode: true,
          disableBatching: false,
          disableRpc: false,
          weight: null,
          internalTimeoutMs: 10_000,
          maxGetLogsBlockSpan: 10_000,
          maxGetLogsAddressBatchSize: null,
          methods: {
            eth_blockNumber: 100,
            eth_call: 100,
            eth_getBlockByNumber: 100,
            eth_getLogs: 100,
            eth_getTransactionReceipt: 100,
          },
          minDelayBetweenCalls: 0,
        } as RpcLimitations,
      }),
    ).toEqual({
      canUseBatchProvider: true,
      maxInputObjsPerBatch: 10,
    });
  });
});
