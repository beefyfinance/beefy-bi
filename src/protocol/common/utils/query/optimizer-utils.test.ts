import { _buildRangeIndex, extractObjsAndRangeFromOptimizerOutput } from "./optimizer-utils";

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
