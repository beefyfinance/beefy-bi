import { explorerLogToERC20TransferEvent } from "./streamContractEventsFromExplorer";

describe("test parsing of explorer log data", () => {
  it("should parse a fantom transfer event", () => {
    const event = {
      address: "0x4c25854f6da3b5848f7b5c71dcb8eee20b292d3e",
      topics: [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x000000000000000000000000d146c053f9ea51199d273c769c4fae2a8e12d7b3",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
      ],
      data: "0x00000000000000000000000000000000000000000000025e00dd7ab9b6665e6e",
      blockNumber: "0x1aa4c53",
      timeStamp: "0x61e17b53",
      gasPrice: "0x3982e23a40",
      gasUsed: "0x3e9ed",
      logIndex: "0xd7",
      transactionHash:
        "0x38a5f8f010d31ce81a075fc04a2c9adf78bd4bd07a08f779bd15c3aed5a718e4",
      transactionIndex: "0x4",
    };
    expect(explorerLogToERC20TransferEvent(event)).toMatchSnapshot();
  });

  it("should parse a bsc transfer event", () => {
    const event = {
      address: "0x13071d48a5fde2735102657e15d1132f92ee8c83",
      topics: [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000009730155214fcad09a4a002f9ca7f9dbf3e706cd7",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
      ],
      data: "0x000000000000000000000000000000000000000000000000248bfe4b25cd809f",
      blockNumber: "0xc0d378",
      timeStamp: "0x6190c567",
      gasPrice: "0x12a05f200",
      gasUsed: "0x3fed4",
      logIndex: "0x21b",
      transactionHash:
        "0xf4ce74f18f980be4b3fbc27f6f2d1774aa119c307ef27927280759559b708d2d",
      transactionIndex: "0x98",
    };
    expect(explorerLogToERC20TransferEvent(event)).toMatchSnapshot();
  });

  it("should parse a metis transfer event", async () => {
    const event = {
      address: "0xea01ca0423acb8476e1d3bae572021c2aa9bd410",
      blockNumber: "0x81916",
      data: "0x00000000000000000000000000000000000000000000000003bc054b9a039800",
      gasPrice: "0x2cb417800",
      gasUsed: "0x6cf87",
      logIndex: "0x4",
      timeStamp: "0x61e81c70",
      topics: [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x000000000000000000000000010da5ff62b6e45f89fa7b2d8ced5a8b5754ec1b",
        null,
      ],
      transactionHash:
        "0x7647c50775d428ae15f8f332e663903231c32ff735433732d9e32327a7390a84",
      transactionIndex: "0x0",
    };

    expect(explorerLogToERC20TransferEvent(event)).toMatchSnapshot();
  });
});
