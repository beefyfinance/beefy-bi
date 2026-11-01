import Decimal from "decimal.js";
import { getUnderlyingCapturePercentage } from "./beefy-vault-stats";

describe("Beefy vault stats", () => {
  it("should compute the capture percentage", () => {
    expect(getUnderlyingCapturePercentage(new Decimal(1), new Decimal(4))).toBe(0.25);
    expect(getUnderlyingCapturePercentage(new Decimal(0), new Decimal(4))).toBe(0);
  });

  it.each([
    ["0/0", 0],
    ["1/0", 1],
  ])("should give null on %s instead of a non finite number", (_, staked) => {
    expect(getUnderlyingCapturePercentage(new Decimal(staked), new Decimal(0))).toBeNull();
  });
});
