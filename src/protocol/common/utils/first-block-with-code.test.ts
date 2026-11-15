import { findFirstBlockWithCode } from "./first-block-with-code";

// a contract deployed at block `deployedAt`, has code from then on
const contractWithCodeFrom = (deployedAt: number) => async (blockNumber: number) => blockNumber >= deployedAt;

describe("findFirstBlockWithCode", () => {
  it("finds the exact deployment block", async () => {
    expect(await findFirstBlockWithCode({ latestBlockNumber: 1_000_000, hasCodeAt: contractWithCodeFrom(123_456) })).toBe(123_456);
  });

  it("finds block 0 when the contract has code from genesis", async () => {
    expect(await findFirstBlockWithCode({ latestBlockNumber: 1_000, hasCodeAt: contractWithCodeFrom(0) })).toBe(0);
  });

  it("finds the latest block when the contract was deployed in it", async () => {
    expect(await findFirstBlockWithCode({ latestBlockNumber: 1_000, hasCodeAt: contractWithCodeFrom(1_000) })).toBe(1_000);
  });

  it("returns null when there is no code at the latest block", async () => {
    expect(await findFirstBlockWithCode({ latestBlockNumber: 1_000, hasCodeAt: async () => false })).toBeNull();
  });

  it("uses about log2(latest) calls", async () => {
    let calls = 0;
    const hasCodeAt = async (n: number) => {
      calls++;
      return n >= 40_000_000;
    };
    expect(await findFirstBlockWithCode({ latestBlockNumber: 57_000_000, hasCodeAt })).toBe(40_000_000);
    expect(calls).toBeLessThanOrEqual(28);
  });
});
