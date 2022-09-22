import { rangeExclude, rangeMerge } from "./range";

describe("range utils", () => {
  it("should compute range exclusions properly", () => {
    expect(rangeExclude({ from: 1, to: 10 }, { from: 50, to: 60 })).toEqual([{ from: 1, to: 10 }]);
    expect(rangeExclude({ from: 1, to: 10 }, { from: -60, to: -50 })).toEqual([{ from: 1, to: 10 }]);
    expect(rangeExclude({ from: 1, to: 10 }, { from: 1, to: 10 })).toEqual([]);
    expect(rangeExclude({ from: 1, to: 10 }, { from: 0, to: 30 })).toEqual([]);
    expect(rangeExclude({ from: 1, to: 10 }, { from: 0, to: 5 })).toEqual([{ from: 6, to: 10 }]);
    expect(rangeExclude({ from: 1, to: 10 }, { from: 5, to: 12 })).toEqual([{ from: 1, to: 4 }]);
    expect(rangeExclude({ from: 1, to: 10 }, { from: 3, to: 6 })).toEqual([
      { from: 1, to: 2 },
      { from: 7, to: 10 },
    ]);
  });

  it("should merge ranges properly", () => {
    expect(
      rangeMerge([
        { from: 1, to: 10 },
        { from: 1, to: 10 },
      ]),
    ).toEqual([{ from: 1, to: 10 }]);
    expect(
      rangeMerge([
        { from: 1, to: 10 },
        { from: 1, to: 10 },
        { from: 1, to: 10 },
      ]),
    ).toEqual([{ from: 1, to: 10 }]);

    expect(
      rangeMerge([
        { from: 1, to: 10 },
        { from: 11, to: 20 },
      ]),
    ).toEqual([{ from: 1, to: 20 }]);

    expect(
      rangeMerge([
        { from: 1, to: 10 },
        { from: 5, to: 15 },
        { from: 7, to: 25 },
      ]),
    ).toEqual([{ from: 1, to: 25 }]);

    expect(
      rangeMerge([
        { from: 40, to: 50 },
        { from: 1, to: 10 },
        { from: 20, to: 30 },
      ]),
    ).toEqual([
      { from: 1, to: 10 },
      { from: 20, to: 30 },
      { from: 40, to: 50 },
    ]);

    expect(
      rangeMerge([
        { from: 1, to: 10 },
        { from: 30, to: 40 },
        { from: 7, to: 25 },
      ]),
    ).toEqual([
      { from: 1, to: 25 },
      { from: 30, to: 40 },
    ]);

    expect(
      rangeMerge([
        { from: 1, to: 10 },
        { from: 10, to: 20 },
        { from: 21, to: 30 },
        { from: 32, to: 40 },
        { from: 41, to: 50 },
      ]),
    ).toEqual([
      { from: 1, to: 30 },
      { from: 32, to: 50 },
    ]);
  });
});
