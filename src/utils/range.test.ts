import { ProgrammerError } from "./programmer-error";
import {
  isInRange,
  rangeArrayExclude,
  rangeEqual,
  rangeExclude,
  rangeExcludeMany,
  rangeMerge,
  rangeSlitToMaxLength,
  rangeSplitManyToMaxLength,
  rangeValueMax,
} from "./range";

describe("range utils: numbers", () => {
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
});

describe("range utils: dates", () => {
  if (process.env.TZ !== "UTC") {
    throw new ProgrammerError("Tests must be run with TZ=UTC");
  }

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
});
