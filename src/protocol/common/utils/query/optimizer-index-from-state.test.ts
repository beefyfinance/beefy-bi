import { createOptimizerIndexFromState } from "./optimizer-index-from-state";

describe("Optimizer create index from state", () => {
  it("should be able to identify distinct blobs and align queries with them", () => {
    expect(
      createOptimizerIndexFromState(
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
      createOptimizerIndexFromState(
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
      createOptimizerIndexFromState(
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
});
