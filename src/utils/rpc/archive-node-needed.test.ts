import { isErrorDueToMissingDataFromNode } from "./archive-node-needed";

describe("test archive node needed error parsing", () => {
  it("should not detect unrelated errors as matching", () => {
    expect(
      isErrorDueToMissingDataFromNode({
        reason: "missing revert data in call exception; Transaction reverted without a reason string",
        code: "CALL_EXCEPTION",
        data: "0x",
        transaction: {
          to: "0xbF07093ccd6adFC3dEB259C557b61E94c1F66945",
          data: "0x77c7b8fc",
          accessList: null,
        },
        error: {
          reason: "processing response error",
          code: "SERVER_ERROR",
          body: '{"jsonrpc":"2.0","id":44,"error":{"code":-32000,"message":"failed to deliver"}}',
          error: {
            code: -32000,
          },
          requestBody:
            '{"method":"eth_call","params":[{"to":"0xbf07093ccd6adfc3deb259c557b61e94c1f66945","data":"0x77c7b8fc"},"0xa7753a"],"id":44,"jsonrpc":"2.0"}',
          requestMethod: "POST",
          url: "https://rpc.ftm.tools",
        },
      }),
    ).toBe(false);
  });

  it("should detect missing errors as matching", () => {
    expect(
      isErrorDueToMissingDataFromNode({
        reason: "missing revert data in call exception; Transaction reverted without a reason string",
        code: "CALL_EXCEPTION",
        data: "0x",
        transaction: {
          to: "0x2C43DBef81ABa6b95799FD2aEc738Cd721ba77f3",
          data: "0x77c7b8fc",
          accessList: null,
        },
        error: {
          reason: "processing response error",
          code: "SERVER_ERROR",
          body: '{"error":{"code":-32000,"message":"This request is not supported because your node is running with state pruning. Run with --pruning=archive."},"id":44,"jsonrpc":"2.0"}',
          error: {
            code: -32000,
          },
          requestBody:
            '{"method":"eth_call","params":[{"to":"0x2c43dbef81aba6b95799fd2aec738cd721ba77f3","data":"0x77c7b8fc"},"0xe38ee4"],"id":44,"jsonrpc":"2.0"}',
          requestMethod: "POST",
          url: "https://rpc.fuse.io",
        },
      }),
    ).toBe(true);

    expect(
      isErrorDueToMissingDataFromNode({
        reason: "missing revert data in call exception; Transaction reverted without a reason string",
        code: "CALL_EXCEPTION",
        data: "0x",
        transaction: {
          to: "0xbF07093ccd6adFC3dEB259C557b61E94c1F66945",
          data: "0x77c7b8fc",
          accessList: null,
        },
        error: {
          reason: "processing response error",
          code: "SERVER_ERROR",
          body: '{"jsonrpc":"2.0","id":44,"error":{"code":-32000,"message":"missing trie node 5c0ee75d9dd668839db2895c93257f6a3a77282740c045d286ff0d1f297ffa24 (path )"}}',
          error: {
            code: -32000,
          },
          requestBody:
            '{"method":"eth_call","params":[{"to":"0xbf07093ccd6adfc3deb259c557b61e94c1f66945","data":"0x77c7b8fc"},"0xa7753a"],"id":44,"jsonrpc":"2.0"}',
          requestMethod: "POST",
          url: "https://rpc.ftm.tools",
        },
      }),
    ).toBe(true);
  });

  it("should detect missing errors as matching", () => {
    // this one is not the exact same error, I just replaced the error body
    expect(
      isErrorDueToMissingDataFromNode({
        reason: "missing revert data in call exception; Transaction reverted without a reason string",
        code: "CALL_EXCEPTION",
        data: "0x",
        transaction: {
          to: "0x2C43DBef81ABa6b95799FD2aEc738Cd721ba77f3",
          data: "0x77c7b8fc",
          accessList: null,
        },
        error: {
          reason: "processing response error",
          code: "SERVER_ERROR",
          body: '{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"missing trie node 0000000000000000000000000000000000000000000000000000000000000000 (path )"}}',
          error: {
            code: -32000,
          },
          requestBody:
            '{"method":"eth_call","params":[{"to":"0x2c43dbef81aba6b95799fd2aec738cd721ba77f3","data":"0x77c7b8fc"},"0xe38ee4"],"id":44,"jsonrpc":"2.0"}',
          requestMethod: "POST",
          url: "https://rpc.fuse.io",
        },
      }),
    ).toBe(true);
  });

  it("should detect missing trie nodes from direct rpc calls", () => {
    expect(
      isErrorDueToMissingDataFromNode({
        jsonrpc: "2.0",
        id: 1,
        error: {
          code: -32000,
          message: "missing trie node df83afcfbf76279a8c108917d7c1f1cdb192a969eecd638d78e6de2bed779a61 (path )",
        },
      }),
    ).toBe(true);
  });
});
