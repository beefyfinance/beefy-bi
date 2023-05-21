import { extractObjsAndRangeFromOptimizerOutput } from "./optimizer-utils";

describe("Optimizer utils", () => {
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
