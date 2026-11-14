import { BlockRef, BlockTimeModel, estimateBlockNumber, findBlockNearTimestamp } from "./block-at-timestamp";

// simulated Base chain: 2s blocks until the Denim change, 0.2s blocks after it
const DAY = 24 * 60 * 60;
const CHANGE_TS = 1793901600; // 2026-11-05 18:00 UTC
const LATEST_NUMBER = 54_000_000;
const LATEST_TS = CHANGE_TS + 9 * DAY;
const CHANGE_BLOCK = LATEST_NUMBER - (LATEST_TS - CHANGE_TS) / 0.2;

const blockTs = (n: number) => (n >= CHANGE_BLOCK ? CHANGE_TS + (n - CHANGE_BLOCK) * 0.2 : CHANGE_TS - (CHANGE_BLOCK - n) * 2);

const latest: BlockRef = { number: LATEST_NUMBER, timestamp: LATEST_TS };
const baseModel: BlockTimeModel = { secondsPerBlock: 0.2, change: { timestamp: CHANGE_TS, secondsPerBlockBefore: 2 } };
const noChangeModel: BlockTimeModel = { secondsPerBlock: 0.2 };

describe("estimateBlockNumber with the base block time change", () => {
  it("is exact for a target after the change", () => {
    const target = LATEST_TS - 2 * DAY;
    const n = estimateBlockNumber({ latest, targetTimestamp: target, model: baseModel });
    expect(Math.abs(blockTs(n) - target)).toBeLessThan(1);
  });

  it("is exact for a target 53 days back, across the change", () => {
    const target = LATEST_TS - 53 * DAY;
    const n = estimateBlockNumber({ latest, targetTimestamp: target, model: baseModel });
    expect(Math.abs(blockTs(n) - target)).toBeLessThan(1);
  });

  it("is exact for a target 327 days back, far before the change", () => {
    const target = LATEST_TS - 327 * DAY;
    const n = estimateBlockNumber({ latest, targetTimestamp: target, model: baseModel });
    expect(Math.abs(blockTs(n) - target)).toBeLessThan(1);
  });

  it("would be far off for a target across the change without the change model", () => {
    const target = LATEST_TS - 53 * DAY;
    const n = estimateBlockNumber({ latest, targetTimestamp: target, model: noChangeModel });
    expect(Math.abs(blockTs(n) - target)).toBeGreaterThan(30 * DAY);
  });

  it("clamps to block 0 when the target is before genesis", () => {
    expect(estimateBlockNumber({ latest, targetTimestamp: 0, model: baseModel })).toBe(0);
  });
});

describe("findBlockNearTimestamp", () => {
  it("returns a block within tolerance using at most 3 getBlock calls for a target across the change", async () => {
    const target = LATEST_TS - 53 * DAY;
    let calls = 0;
    const block = await findBlockNearTimestamp({
      latest,
      targetTimestamp: target,
      model: baseModel,
      maxCorrections: 2,
      toleranceSeconds: DAY,
      getBlock: async (n) => {
        calls++;
        return { number: n, timestamp: blockTs(n) };
      },
    });
    expect(Math.abs(block.timestamp - target)).toBeLessThanOrEqual(DAY);
    expect(block.timestamp).toBe(blockTs(block.number)); // number and timestamp always match
    expect(calls).toBeLessThanOrEqual(3);
  });

  it("limits calls and still pairs number and timestamp with a wrong model", async () => {
    const target = LATEST_TS - 53 * DAY;
    let calls = 0;
    const block = await findBlockNearTimestamp({
      latest,
      targetTimestamp: target,
      model: noChangeModel,
      maxCorrections: 2,
      toleranceSeconds: DAY,
      getBlock: async (n) => {
        calls++;
        return { number: n, timestamp: blockTs(n) };
      },
    });
    expect(calls).toBeLessThanOrEqual(3);
    expect(block.timestamp).toBe(blockTs(block.number));
  });
});
