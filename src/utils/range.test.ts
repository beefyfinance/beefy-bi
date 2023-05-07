import { cloneDeep } from "lodash";
import { ProgrammerError } from "./programmer-error";
import {
  getRangeSize,
  isInRange,
  isValidRange,
  rangeArrayExclude,
  rangeArrayOverlap,
  rangeCovering,
  rangeEqual,
  rangeExclude,
  rangeExcludeMany,
  rangeInclude,
  rangeIntersect,
  rangeManyOverlap,
  rangeMerge,
  rangeOverlap,
  rangeSlitToMaxLength,
  rangeSortedArrayExclude,
  rangeSortedSplitManyToMaxLengthAndTakeSome,
  rangeSplitManyToMaxLength,
  rangeValueMax,
} from "./range";

describe("range utils: numbers", () => {
  it("should identify a valid range", () => {
    expect(isValidRange({ from: 0, to: 0 })).toBe(true);
    expect(isValidRange({ from: 0, to: 10 })).toBe(true);
    expect(isValidRange({ from: 11, to: 10 })).toBe(false);
  });

  it("should compute a range size", () => {
    expect(getRangeSize({ from: 0, to: 0 })).toBe(1);
    expect(getRangeSize({ from: 0, to: 10 })).toBe(11);
    expect(getRangeSize({ from: 11, to: 10 })).toBe(0);
  });

  it("should compare 2 ranges", () => {
    expect(rangeEqual({ from: 1, to: 2 }, { from: 1, to: 2 })).toBe(true);
    expect(rangeEqual({ from: 1, to: 2 }, { from: 1, to: 3 })).toBe(false);
  });

  it("should return max value", () => {
    expect(rangeValueMax([1, 2, 6, 3, 4])).toBe(6);
    expect(rangeValueMax([])).toBe(undefined);
  });

  it("should test if a value is in the range", () => {
    expect(isInRange({ from: 1, to: 2 }, 1)).toBe(true);
    expect(isInRange({ from: 1, to: 2 }, 2)).toBe(true);
    expect(isInRange({ from: 1, to: 2 }, 3)).toBe(false);
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

  it("should compute range array exclusions properly (but with a faster method)", () => {
    expect(
      rangeSortedArrayExclude(
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
      rangeSortedArrayExclude(
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

  it("should be able to split a range array to a maximum length", () => {
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

  it("should test for range inclusion", () => {
    expect(rangeInclude({ from: 1, to: 10 }, { from: 1, to: 10 })).toBe(true);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 1, to: 9 })).toBe(true);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 2, to: 10 })).toBe(true);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 2, to: 9 })).toBe(true);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 0, to: 11 })).toBe(false);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 11, to: 12 })).toBe(false);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 0, to: 10 })).toBe(false);
    expect(rangeInclude({ from: 1, to: 10 }, { from: 0, to: 11 })).toBe(false);
    expect(rangeInclude({ from: 1, to: 10 }, { from: -1, to: 0 })).toBe(false);
    expect(rangeInclude({ from: 1, to: 10 }, { from: -1, to: 3 })).toBe(false);
  });

  it("should test for range strict overlap", () => {
    expect(rangeOverlap({ from: 1, to: 10 }, { from: 1, to: 10 })).toBe(true);
    expect(rangeOverlap({ from: 1, to: 10 }, { from: 1, to: 9 })).toBe(true);
    expect(rangeOverlap({ from: 1, to: 10 }, { from: 2, to: 10 })).toBe(true);
    expect(rangeOverlap({ from: 1, to: 10 }, { from: 2, to: 9 })).toBe(true);
    expect(rangeOverlap({ from: 1, to: 10 }, { from: 0, to: 11 })).toBe(true);
    expect(rangeOverlap({ from: 1, to: 10 }, { from: 11, to: 12 })).toBe(false);
    expect(rangeOverlap({ from: 1, to: 10 }, { from: -1, to: 0 })).toBe(false);
  });

  it("should test for range array overlap", () => {
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: 1, to: 10 },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: 1, to: 9 },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: 2, to: 10 },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: 2, to: 9 },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: 0, to: 11 },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: 11, to: 12 },
      ]),
    ).toBe(false);
    expect(
      rangeArrayOverlap([
        { from: 1, to: 10 },
        { from: -1, to: 0 },
      ]),
    ).toBe(false);
  });

  it("should be able to detect overlaps in 2 range arrays", () => {
    expect(rangeManyOverlap([{ from: 1, to: 100 }], [{ from: 1, to: 10 }])).toBe(true);
    expect(rangeManyOverlap([{ from: 1, to: 100 }], [{ from: 1, to: 9 }])).toBe(true);
    expect(rangeManyOverlap([{ from: 1, to: 100 }], [{ from: 2, to: 10 }])).toBe(true);

    // thanks chatgpt
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 7, to: 10 },
        ],
        [
          { from: 2, to: 6 },
          { from: 8, to: 9 },
        ],
      ),
    ).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 7, to: 10 },
        ],
        [
          { from: 5, to: 8 },
          { from: 11, to: 12 },
        ],
      ),
    ).toBe(true);
    expect(rangeManyOverlap([{ from: 1, to: 5 }], [{ from: 5, to: 10 }])).toBe(true);
    expect(rangeManyOverlap([{ from: 1, to: 5 }], [{ from: 3, to: 4 }])).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 6, to: 10 },
        ],
        [{ from: 5, to: 7 }],
      ),
    ).toBe(true);
    expect(rangeManyOverlap([], [])).toBe(false);
    expect(rangeManyOverlap([], [{ from: 1, to: 5 }])).toBe(false);
    expect(rangeManyOverlap([{ from: 1, to: 5 }], [])).toBe(false);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 6, to: 10 },
        ],
        [{ from: 11, to: 12 }],
      ),
    ).toBe(false);
    expect(rangeManyOverlap([{ from: -5, to: -1 }], [{ from: -4, to: 0 }])).toBe(true);
    expect(rangeManyOverlap([{ from: 0, to: 0 }], [{ from: 0, to: 0 }])).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 7, to: 10 },
          { from: 11, to: 15 },
        ],
        [
          { from: 2, to: 6 },
          { from: 8, to: 9 },
          { from: 14, to: 16 },
        ],
      ),
    ).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 7, to: 10 },
          { from: 11, to: 15 },
        ],
        [
          { from: 6, to: 8 },
          { from: 16, to: 18 },
          { from: 20, to: 25 },
        ],
      ),
    ).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 7, to: 10 },
        ],
        [
          { from: 11, to: 15 },
          { from: 16, to: 20 },
        ],
      ),
    ).toBe(false);
    expect(
      rangeManyOverlap(
        [
          { from: 1, to: 5 },
          { from: 7, to: 10 },
          { from: 11, to: 15 },
        ],
        [
          { from: 16, to: 20 },
          { from: 21, to: 25 },
        ],
      ),
    ).toBe(false);
    expect(rangeManyOverlap([{ from: 1, to: 5 }], [{ from: 6, to: 10 }])).toBe(false);
  });

  it("should be able to intersect ranges", () => {
    expect(rangeIntersect([], { from: 10, to: 50 })).toEqual([]);
    expect(rangeIntersect([{ from: 0, to: 5 }], { from: 10, to: 50 })).toEqual([]);
    expect(
      rangeIntersect(
        [
          { from: 0, to: 5 },
          { from: 4, to: 9 },
        ],
        { from: 10, to: 50 },
      ),
    ).toEqual([]);

    expect(
      rangeIntersect(
        [
          { from: 0, to: 15 },
          { from: 17, to: 25 },
          { from: 40, to: 100 },
        ],
        { from: 10, to: 50 },
      ),
    ).toEqual([
      { from: 10, to: 15 },
      { from: 17, to: 25 },
      { from: 40, to: 50 },
    ]);
  });

  it("should create a covering range", () => {
    expect(() => rangeCovering([])).toThrow(ProgrammerError);
    expect(rangeCovering([{ from: 10, to: 15 }])).toEqual({ from: 10, to: 15 });
    expect(
      rangeCovering([
        { from: 10, to: 15 },
        { from: 26, to: 30 },
      ]),
    ).toEqual({ from: 10, to: 30 });
    expect(
      rangeCovering([
        { from: 10, to: 15 },
        { from: 35, to: 47 },
        { from: 26, to: 30 },
      ]),
    ).toEqual({ from: 10, to: 47 });
  });

  it("should provide a fast method to do sort+split+merge+take", () => {
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 10 }], 5, 1)).toEqual([{ from: 1, to: 5 }]);
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 100 }], 5, 5, "desc")).toEqual([
      { from: 96, to: 100 },
      { from: 91, to: 95 },
      { from: 86, to: 90 },
      { from: 81, to: 85 },
      { from: 76, to: 80 },
    ]);

    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 100 }], 5, 5, "asc")).toEqual([
      { from: 1, to: 5 },
      { from: 6, to: 10 },
      { from: 11, to: 15 },
      { from: 16, to: 20 },
      { from: 21, to: 25 },
    ]);

    // should be able to merge ranges when needed
    expect(
      rangeSortedSplitManyToMaxLengthAndTakeSome(
        [
          { from: 1, to: 3 },
          { from: 2, to: 4 },
          { from: 3, to: 9 },
          { from: 50, to: 100 },
        ],
        5,
        5,
        "asc",
      ),
    ).toEqual([
      { from: 1, to: 5 },
      { from: 6, to: 9 },
      { from: 50, to: 54 },
      { from: 55, to: 59 },
      { from: 60, to: 64 },
    ]);

    // should be able to merge ranges when needed
    expect(
      rangeSortedSplitManyToMaxLengthAndTakeSome(
        [
          { from: 1, to: 50 },
          { from: 91, to: 95 },
          { from: 96, to: 97 },
          { from: 97, to: 100 },
        ],
        5,
        5,
        "desc",
      ),
    ).toEqual([
      { from: 96, to: 100 },
      { from: 91, to: 95 },
      { from: 46, to: 50 },
      { from: 41, to: 45 },
      { from: 36, to: 40 },
    ]);

    // it can handle weird cases
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([], 5, 5, "desc")).toEqual([]);
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 1 }], 0, 5, "desc")).toEqual([]);
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 1 }], 5, 0, "desc")).toEqual([]);
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 1 }], 5, 5, "desc")).toEqual([{ from: 1, to: 1 }]);

    // hopefully it's really fast
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: 1, to: 10000 }], 1, 10000, "desc")).toHaveLength(10000);

    // and it doesn't mutate the input
    const input = [
      { from: 1, to: 50 },
      { from: 91, to: 95 },
      { from: 96, to: 97 },
      { from: 97, to: 100 },
    ];
    const expectedInput = cloneDeep(input);
    rangeSortedSplitManyToMaxLengthAndTakeSome(input, 5, 5, "desc");
    expect(input).toEqual(expectedInput);
  });
});

describe("range utils: dates", () => {
  if (process.env.TZ !== "UTC") {
    throw new ProgrammerError("Tests must be run with TZ=UTC");
  }

  it("should identify a valid range", () => {
    expect(isValidRange({ from: new Date("2000-01-01"), to: new Date("2000-01-01") })).toBe(true);
    expect(isValidRange({ from: new Date("2000-01-01"), to: new Date("2000-01-02") })).toBe(true);
    expect(isValidRange({ from: new Date("2000-01-02"), to: new Date("2000-01-01") })).toBe(false);
  });

  it("should compute a range size", () => {
    expect(getRangeSize({ from: new Date("2000-01-01"), to: new Date("2000-01-01") })).toBe(1);
    expect(getRangeSize({ from: new Date("2000-01-01"), to: new Date("2000-01-02") })).toBe(86400001);
    expect(getRangeSize({ from: new Date("2000-01-02"), to: new Date("2000-01-01") })).toBe(0);
  });

  it("should compare 2 ranges", () => {
    expect(
      rangeEqual(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ),
    ).toBe(true);
    expect(
      rangeEqual(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.998Z") },
      ),
    ).toBe(false);
  });

  it("should return max value", () => {
    expect(
      rangeValueMax([new Date("2000-01-01"), new Date("2000-01-02"), new Date("2000-01-06"), new Date("2000-01-03"), new Date("2000-01-04")]),
    ).toEqual(new Date("2000-01-06"));
    expect(rangeValueMax([])).toBe(undefined);
  });

  it("should test if a value is in the range", () => {
    expect(isInRange({ from: new Date("2000-01-01"), to: new Date("2000-01-02") }, new Date("2000-01-01"))).toBe(true);
    expect(isInRange({ from: new Date("2000-01-01"), to: new Date("2000-01-02") }, new Date("2000-01-02"))).toBe(true);
    expect(isInRange({ from: new Date("2000-01-01"), to: new Date("2000-01-02") }, new Date("2000-01-03"))).toBe(false);
  });

  it("should compute range exclusions properly", () => {
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2010-01-01"), to: new Date("2010-01-02") },
      ),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }]);
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("1990-01-01"), to: new Date("1990-01-02") },
      ),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }]);
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ),
    ).toEqual([]);
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("1999-01-01"), to: new Date("2010-01-01") },
      ),
    ).toEqual([]);
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("1999-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      ),
    ).toEqual([{ from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-01T23:59:59.999Z") }]);
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-03") },
      ),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") }]);
    expect(
      rangeExclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-01T13:00:00") },
      ),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T13:00:00.001Z"), to: new Date("2000-01-01T23:59:59.999Z") },
    ]);
  });

  it("should compute multiple range exclusions properly", () => {
    expect(
      rangeExcludeMany({ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }, [
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ]),
    ).toEqual([]);
    expect(
      rangeExcludeMany({ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }, [
        { from: new Date("2010-01-01"), to: new Date("2020-01-01") },
      ]),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }]);
    expect(
      rangeExcludeMany({ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }, [
        { from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-01T12:59:59.999Z") },
        { from: new Date("2000-01-01T15:00:00"), to: new Date("2000-01-01T15:59:59.999Z") },
      ]),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T13:00:00.000Z"), to: new Date("2000-01-01T14:59:59.999Z") },
      { from: new Date("2000-01-01T16:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
    ]);
    expect(
      rangeExcludeMany({ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }, [
        { from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-01T12:59:59.999Z") },
        { from: new Date("2000-01-01T15:00:00"), to: new Date("2000-01-01T15:59:59.999Z") },
        { from: new Date("2010-01-01"), to: new Date("2020-01-01") },
      ]),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T13:00:00.000Z"), to: new Date("2000-01-01T14:59:59.999Z") },
      { from: new Date("2000-01-01T16:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
    ]);
  });

  it("should compute range array exclusions properly", () => {
    expect(
      rangeArrayExclude(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
          { from: new Date("2000-01-05"), to: new Date("2000-01-05T23:59:59.999Z") },
        ],
        [
          { from: new Date("2010-01-01"), to: new Date("2020-01-01") },
          { from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-01T12:59:59.999Z") },
          { from: new Date("2000-01-05T12:00:00"), to: new Date("2000-01-05T12:59:59.999Z") },
        ],
      ),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T13:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
      { from: new Date("2000-01-05"), to: new Date("2000-01-05T11:59:59.999Z") },
      { from: new Date("2000-01-05T13:00:00.000Z"), to: new Date("2000-01-05T23:59:59.999Z") },
    ]);

    expect(
      rangeArrayExclude(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
          { from: new Date("2000-01-05"), to: new Date("2000-01-05T23:59:59.999Z") },
        ],
        [
          { from: new Date("1999-01-01"), to: new Date("2000-01-02") },
          { from: new Date("2000-01-05T08:00:00"), to: new Date("2000-01-05T08:59:59.999Z") },
          { from: new Date("2000-01-05T15:00:00"), to: new Date("2000-01-05T15:59:59.999Z") },
        ],
      ),
    ).toEqual([
      { from: new Date("2000-01-05"), to: new Date("2000-01-05T07:59:59.999Z") },
      { from: new Date("2000-01-05T09:00:00"), to: new Date("2000-01-05T14:59:59.999Z") },
      { from: new Date("2000-01-05T16:00:00"), to: new Date("2000-01-05T23:59:59.999Z") },
    ]);
  });

  it("should compute range array exclusions properly", () => {
    expect(
      rangeSortedArrayExclude(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
          { from: new Date("2000-01-05"), to: new Date("2000-01-05T23:59:59.999Z") },
        ],
        [
          { from: new Date("2010-01-01"), to: new Date("2020-01-01") },
          { from: new Date("2000-01-01T12:00:00"), to: new Date("2000-01-01T12:59:59.999Z") },
          { from: new Date("2000-01-05T12:00:00"), to: new Date("2000-01-05T12:59:59.999Z") },
        ],
      ),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T13:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
      { from: new Date("2000-01-05"), to: new Date("2000-01-05T11:59:59.999Z") },
      { from: new Date("2000-01-05T13:00:00.000Z"), to: new Date("2000-01-05T23:59:59.999Z") },
    ]);

    expect(
      rangeSortedArrayExclude(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
          { from: new Date("2000-01-05"), to: new Date("2000-01-05T23:59:59.999Z") },
        ],
        [
          { from: new Date("1999-01-01"), to: new Date("2000-01-02") },
          { from: new Date("2000-01-05T08:00:00"), to: new Date("2000-01-05T08:59:59.999Z") },
          { from: new Date("2000-01-05T15:00:00"), to: new Date("2000-01-05T15:59:59.999Z") },
        ],
      ),
    ).toEqual([
      { from: new Date("2000-01-05"), to: new Date("2000-01-05T07:59:59.999Z") },
      { from: new Date("2000-01-05T09:00:00"), to: new Date("2000-01-05T14:59:59.999Z") },
      { from: new Date("2000-01-05T16:00:00"), to: new Date("2000-01-05T23:59:59.999Z") },
    ]);
  });

  it("should merge ranges properly", () => {
    expect(
      rangeMerge([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ]),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }]);
    expect(
      rangeMerge([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ]),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }]);

    expect(
      rangeMerge([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-02"), to: new Date("2000-01-02T23:59:59.999Z") },
      ]),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-02T23:59:59.999Z") }]);

    expect(
      rangeMerge([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01T10:59:59.999Z"), to: new Date("2000-01-02T10:59:59.999Z") },
        { from: new Date("2000-01-01T12:59:59.999Z"), to: new Date("2000-01-02T08:59:59.999Z") },
      ]),
    ).toEqual([{ from: new Date("2000-01-01"), to: new Date("2000-01-02T10:59:59.999Z") }]);

    expect(
      rangeMerge([
        { from: new Date("2000-01-10"), to: new Date("2000-01-10T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-07"), to: new Date("2000-01-07T23:59:59.999Z") },
      ]),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      { from: new Date("2000-01-07"), to: new Date("2000-01-07T23:59:59.999Z") },
      { from: new Date("2000-01-10"), to: new Date("2000-01-10T23:59:59.999Z") },
    ]);

    expect(
      rangeMerge([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-10"), to: new Date("2000-01-10T23:59:59.999Z") },
        { from: new Date("2000-01-01T15:59:59.999Z"), to: new Date("2000-01-02T13:59:59.999Z") },
      ]),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-02T13:59:59.999Z") },
      { from: new Date("2000-01-10"), to: new Date("2000-01-10T23:59:59.999Z") },
    ]);

    expect(
      rangeMerge([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-02"), to: new Date("2000-01-02T23:59:59.999Z") },
        { from: new Date("2000-01-03T00:00:00.002Z"), to: new Date("2000-01-03T23:59:59.999Z") },
        { from: new Date("2000-01-04"), to: new Date("2000-01-04T23:59:59.999Z") },
        { from: new Date("2000-01-05"), to: new Date("2000-01-05T23:59:59.999Z") },
      ]),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-02T23:59:59.999Z") },
      { from: new Date("2000-01-03T00:00:00.002Z"), to: new Date("2000-01-05T23:59:59.999Z") },
    ]);
  });

  it("should be able to split a range to a maximum length", () => {
    const oneHourMs = 60 * 60 * 1000;
    expect(rangeSlitToMaxLength({ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }, oneHourMs * 12)).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T12:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
    ]);

    expect(rangeSlitToMaxLength({ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }, oneHourMs * 6)).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T05:59:59.999Z") },
      { from: new Date("2000-01-01T06:00:00.000Z"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T12:00:00.000Z"), to: new Date("2000-01-01T17:59:59.999Z") },
      { from: new Date("2000-01-01T18:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
    ]);
  });

  it("should be able to split a range array to a maximum length", () => {
    const oneHourMs = 60 * 60 * 1000;
    expect(rangeSplitManyToMaxLength([{ from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") }], oneHourMs * 12)).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T12:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
    ]);

    expect(
      rangeSplitManyToMaxLength(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
          { from: new Date("2000-01-05"), to: new Date("2000-01-05T02:59:59.999Z") },
        ],
        oneHourMs * 6,
      ),
    ).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T05:59:59.999Z") },
      { from: new Date("2000-01-01T06:00:00.000Z"), to: new Date("2000-01-01T11:59:59.999Z") },
      { from: new Date("2000-01-01T12:00:00.000Z"), to: new Date("2000-01-01T17:59:59.999Z") },
      { from: new Date("2000-01-01T18:00:00.000Z"), to: new Date("2000-01-01T23:59:59.999Z") },
      { from: new Date("2000-01-05"), to: new Date("2000-01-05T02:59:59.999Z") },
    ]);
  });

  it("should test for range inclusion", () => {
    expect(
      rangeInclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ),
    ).toBe(true);
    expect(
      rangeInclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01T10:00:00.000Z"), to: new Date("2000-01-01T12:00:00.000Z") },
      ),
    ).toBe(true);
    expect(
      rangeInclude({ from: new Date("2000-01-01"), to: new Date("2000-01-02") }, { from: new Date("1999-01-01"), to: new Date("2022-01-01") }),
    ).toBe(false);
    expect(
      rangeInclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T10:00:00.000Z") },
      ),
    ).toBe(true);
    expect(
      rangeInclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01T10:00:00.000Z"), to: new Date("2000-01-02") },
      ),
    ).toBe(true);
    expect(
      rangeInclude(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-02T00:00:00.001Z"), to: new Date("2000-01-03") },
      ),
    ).toBe(false);
  });

  it("should test for range strict overlap", () => {
    expect(
      rangeOverlap(
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ),
    ).toBe(true);
    expect(
      rangeOverlap(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01T10:00:00.000Z"), to: new Date("2000-01-01T12:00:00.000Z") },
      ),
    ).toBe(true);
    expect(
      rangeOverlap({ from: new Date("2000-01-01"), to: new Date("2000-01-02") }, { from: new Date("1999-01-01"), to: new Date("2022-01-01") }),
    ).toBe(true);
    expect(
      rangeOverlap(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T10:00:00.000Z") },
      ),
    ).toBe(true);
    expect(
      rangeOverlap(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01T10:00:00.000Z"), to: new Date("2000-01-02") },
      ),
    ).toBe(true);
    expect(
      rangeOverlap(
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-02T00:00:00.001Z"), to: new Date("2000-01-03") },
      ),
    ).toBe(false);
  });

  it("should test for range array overlap", () => {
    expect(
      rangeArrayOverlap([
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
        { from: new Date("2000-01-01"), to: new Date("2000-01-01T23:59:59.999Z") },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-01T10:00:00.000Z"), to: new Date("2000-01-01T12:00:00.000Z") },
      ]),
    ).toBe(true);

    expect(
      rangeArrayOverlap([
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("1999-01-01"), to: new Date("2022-01-01") },
      ]),
    ).toBe(true);
    expect(
      rangeArrayOverlap([
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-03"), to: new Date("2000-01-04") },
      ]),
    ).toBe(false);
    expect(
      rangeArrayOverlap([
        { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
        { from: new Date("2000-01-02T00:00:00.001Z"), to: new Date("2000-01-03") },
      ]),
    ).toBe(false);
  });

  it("should test for range array many overlap", () => {
    expect(
      rangeManyOverlap(
        [
          { from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) },
          { from: new Date(2020, 1, 7), to: new Date(2020, 1, 10) },
        ],
        [
          { from: new Date(2020, 1, 2), to: new Date(2020, 1, 6) },
          { from: new Date(2020, 1, 8), to: new Date(2020, 1, 9) },
        ],
      ),
    ).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) },
          { from: new Date(2020, 1, 7), to: new Date(2020, 1, 10) },
        ],
        [
          { from: new Date(2020, 1, 5), to: new Date(2020, 1, 8) },
          { from: new Date(2020, 1, 11), to: new Date(2020, 1, 12) },
        ],
      ),
    ).toBe(true);
    expect(
      rangeManyOverlap([{ from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) }], [{ from: new Date(2020, 1, 5), to: new Date(2020, 1, 10) }]),
    ).toBe(true);
    expect(
      rangeManyOverlap([{ from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) }], [{ from: new Date(2020, 1, 3), to: new Date(2020, 1, 4) }]),
    ).toBe(true);
    expect(
      rangeManyOverlap(
        [
          { from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) },
          { from: new Date(2020, 1, 6), to: new Date(2020, 1, 10) },
        ],
        [{ from: new Date(2020, 1, 5), to: new Date(2020, 1, 7) }],
      ),
    ).toBe(true);
    expect(rangeManyOverlap([], [])).toBe(false);
    expect(rangeManyOverlap([], [{ from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) }])).toBe(false);
    expect(rangeManyOverlap([{ from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) }], [])).toBe(false);
    expect(
      rangeManyOverlap(
        [
          { from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) },
          { from: new Date(2020, 1, 7), to: new Date(2020, 1, 10) },
        ],
        [
          { from: new Date(2020, 1, 11), to: new Date(2020, 1, 15) },
          { from: new Date(2020, 1, 16), to: new Date(2020, 1, 20) },
        ],
      ),
    ).toBe(false);
    expect(
      rangeManyOverlap([{ from: new Date(2020, 1, 1), to: new Date(2020, 1, 5) }], [{ from: new Date(2020, 1, 6), to: new Date(2020, 1, 10) }]),
    ).toBe(false);
  });

  it("should be able to intersect ranges", () => {
    expect(rangeIntersect([], { from: new Date("2000-01-10"), to: new Date("2000-02-20") })).toEqual([]);
    expect(
      rangeIntersect([{ from: new Date("2000-01-01"), to: new Date("2000-01-05") }], { from: new Date("2000-01-10"), to: new Date("2000-02-20") }),
    ).toEqual([]);
    expect(
      rangeIntersect(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-05") },
          { from: new Date("2000-01-04"), to: new Date("2000-01-09") },
        ],
        { from: new Date("2000-01-10"), to: new Date("2000-02-20") },
      ),
    ).toEqual([]);

    expect(
      rangeIntersect(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-15") },
          { from: new Date("2000-01-17"), to: new Date("2000-01-25") },
          { from: new Date("2000-02-10"), to: new Date("2000-05-01") },
        ],
        { from: new Date("2000-01-10"), to: new Date("2000-02-20") },
      ),
    ).toEqual([
      { from: new Date("2000-01-10"), to: new Date("2000-01-15") },
      { from: new Date("2000-01-17"), to: new Date("2000-01-25") },
      { from: new Date("2000-02-10"), to: new Date("2000-02-20") },
    ]);
  });

  it("should create a covering range", () => {
    expect(() => rangeCovering([])).toThrow(ProgrammerError);
    expect(rangeCovering([{ from: new Date("2000-01-10"), to: new Date("2000-01-15") }])).toEqual({
      from: new Date("2000-01-10"),
      to: new Date("2000-01-15"),
    });
    expect(
      rangeCovering([
        { from: new Date("2000-01-10"), to: new Date("2000-01-15") },
        { from: new Date("2000-01-26"), to: new Date("2000-01-30") },
      ]),
    ).toEqual({
      from: new Date("2000-01-10"),
      to: new Date("2000-01-30"),
    });
    expect(
      rangeCovering([
        { from: new Date("2000-01-10"), to: new Date("2000-01-15") },
        { from: new Date("2000-02-01"), to: new Date("2000-02-07") },
        { from: new Date("2000-01-26"), to: new Date("2000-01-30") },
      ]),
    ).toEqual({
      from: new Date("2000-01-10"),
      to: new Date("2000-02-07"),
    });
  });

  it("should provide a fast method to do sort+split+merge+take", () => {
    const oneHourMs = 60 * 60 * 1000;
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: new Date("2000-01-01"), to: new Date("2000-01-02") }], oneHourMs, 1)).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01T00:59:59.999Z") },
    ]);
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: new Date("2000-01-01"), to: new Date("2000-01-02") }], oneHourMs, 5, "desc")).toEqual([
      { from: new Date("2000-01-01T23:00:00.001Z"), to: new Date("2000-01-02") },
      { from: new Date("2000-01-01T22:00:00.001Z"), to: new Date("2000-01-01T23:00:00") },
      { from: new Date("2000-01-01T21:00:00.001Z"), to: new Date("2000-01-01T22:00:00") },
      { from: new Date("2000-01-01T20:00:00.001Z"), to: new Date("2000-01-01T21:00:00") },
      { from: new Date("2000-01-01T19:00:00.001Z"), to: new Date("2000-01-01T20:00:00") },
    ]);

    expect(
      rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: new Date("2000-01-01"), to: new Date("2000-01-02") }], oneHourMs * 2, 5, "asc"),
    ).toEqual([
      { from: new Date("2000-01-01T00:00:00.000Z"), to: new Date("2000-01-01T01:59:59.999Z") },
      { from: new Date("2000-01-01T02:00:00.000Z"), to: new Date("2000-01-01T03:59:59.999Z") },
      { from: new Date("2000-01-01T04:00:00.000Z"), to: new Date("2000-01-01T05:59:59.999Z") },
      { from: new Date("2000-01-01T06:00:00.000Z"), to: new Date("2000-01-01T07:59:59.999Z") },
      { from: new Date("2000-01-01T08:00:00.000Z"), to: new Date("2000-01-01T09:59:59.999Z") },
    ]);

    expect(
      rangeSortedSplitManyToMaxLengthAndTakeSome(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
          { from: new Date("2000-01-05"), to: new Date("2000-01-06") },
        ],
        oneHourMs,
        5,
        "desc",
      ),
    ).toEqual([
      { from: new Date("2000-01-05T23:00:00.001Z"), to: new Date("2000-01-06") },
      { from: new Date("2000-01-05T22:00:00.001Z"), to: new Date("2000-01-05T23:00:00") },
      { from: new Date("2000-01-05T21:00:00.001Z"), to: new Date("2000-01-05T22:00:00") },
      { from: new Date("2000-01-05T20:00:00.001Z"), to: new Date("2000-01-05T21:00:00") },
      { from: new Date("2000-01-05T19:00:00.001Z"), to: new Date("2000-01-05T20:00:00") },
    ]);

    // should be able to merge ranges when needed
    expect(
      rangeSortedSplitManyToMaxLengthAndTakeSome(
        [
          { from: new Date("2000-01-01"), to: new Date("2000-01-02") },
          { from: new Date("2000-01-02"), to: new Date("2000-01-03") },
        ],
        oneHourMs * 12,
        5,
        "desc",
      ),
    ).toEqual([
      { from: new Date("2000-01-02T12:00:00.001Z"), to: new Date("2000-01-03") },
      { from: new Date("2000-01-02T00:00:00.001Z"), to: new Date("2000-01-02T12:00:00") },
      { from: new Date("2000-01-01T12:00:00.001Z"), to: new Date("2000-01-02T00:00:00") },
      { from: new Date("2000-01-01T00:00:00.001Z"), to: new Date("2000-01-01T12:00:00") },
      { from: new Date("2000-01-01T00:00:00.000Z"), to: new Date("2000-01-01T00:00:00") },
    ]);

    // it can handle weird cases
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([], 5, 5, "desc")).toEqual([]);
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: new Date("2000-01-01"), to: new Date("2000-01-01") }], oneHourMs, 5, "desc")).toEqual([
      { from: new Date("2000-01-01"), to: new Date("2000-01-01") },
    ]);

    // hopefully it's really fast
    expect(rangeSortedSplitManyToMaxLengthAndTakeSome([{ from: new Date("2000-01-01"), to: new Date("2035-01-01") }], 1, 10000, "desc")).toHaveLength(
      10000,
    );
  });
});
