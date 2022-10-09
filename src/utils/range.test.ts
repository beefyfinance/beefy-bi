import { rangeArrayExclude, rangeEqual, rangeExclude, rangeExcludeMany, rangeMerge, rangeSlitToMaxLength, rangeSplitManyToMaxLength } from "./range";

describe("range utils", () => {
  it("should compare 2 ranges", () => {
    expect(rangeEqual({ from: 1, to: 2 }, { from: 1, to: 2 })).toBe(true);
    expect(rangeEqual({ from: 1, to: 2 }, { from: 1, to: 3 })).toBe(false);
  });

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

  it("should compute multiple range exclusions properly", () => {
    expect(rangeExcludeMany({ from: 1, to: 10 }, [{ from: 1, to: 10 }])).toEqual([]);
    expect(rangeExcludeMany({ from: 1, to: 10 }, [{ from: 100, to: 1001 }])).toEqual([{ from: 1, to: 10 }]);
    expect(
      rangeExcludeMany({ from: 1, to: 10 }, [
        { from: 2, to: 3 },
        { from: 5, to: 6 },
      ]),
    ).toEqual([
      { from: 1, to: 1 },
      { from: 4, to: 4 },
      { from: 7, to: 10 },
    ]);
    expect(
      rangeExcludeMany({ from: 1, to: 10 }, [
        { from: 2, to: 3 },
        { from: 5, to: 6 },
        { from: 100, to: 1001 },
      ]),
    ).toEqual([
      { from: 1, to: 1 },
      { from: 4, to: 4 },
      { from: 7, to: 10 },
    ]);
  });

  it("should compute range array exclusions properly", () => {
    expect(
      rangeArrayExclude(
        [
          { from: 1, to: 10 },
          { from: 15, to: 35 },
        ],
        [
          { from: 50, to: 60 },
          { from: 4, to: 5 },
          { from: 20, to: 21 },
        ],
      ),
    ).toEqual([
      { from: 1, to: 3 },
      { from: 6, to: 10 },
      { from: 15, to: 19 },
      { from: 22, to: 35 },
    ]);

    expect(
      rangeArrayExclude(
        [
          { from: 1, to: 10 },
          { from: 15, to: 35 },
        ],
        [
          { from: 0, to: 13 },
          { from: 17, to: 20 },
          { from: 23, to: 25 },
        ],
      ),
    ).toEqual([
      { from: 15, to: 16 },
      { from: 21, to: 22 },
      { from: 26, to: 35 },
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

  it("should be able to split a range to a maximum length", () => {
    expect(rangeSlitToMaxLength({ from: 1, to: 10 }, 5)).toEqual([
      { from: 1, to: 5 },
      { from: 6, to: 10 },
    ]);

    expect(rangeSlitToMaxLength({ from: 1, to: 20 }, 5)).toEqual([
      { from: 1, to: 5 },
      { from: 6, to: 10 },
      { from: 11, to: 15 },
      { from: 16, to: 20 },
    ]);
  });

  it("should be able to split a range to a maximum length", () => {
    expect(rangeSplitManyToMaxLength([{ from: 1, to: 10 }], 5)).toEqual([
      { from: 1, to: 5 },
      { from: 6, to: 10 },
    ]);

    expect(
      rangeSplitManyToMaxLength(
        [
          { from: 1, to: 18 },
          { from: 20, to: 27 },
        ],
        5,
      ),
    ).toEqual([
      { from: 1, to: 5 },
      { from: 6, to: 10 },
      { from: 11, to: 15 },
      { from: 16, to: 18 },
      { from: 20, to: 24 },
      { from: 25, to: 27 },
    ]);
  });
});
