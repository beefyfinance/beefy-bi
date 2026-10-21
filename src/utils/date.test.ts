import { isoStringToDate } from "./date";

describe("date utils: iso string", () => {
  it("should parse an iso string with timezones properly", () => {
    expect(isoStringToDate("2021-08-01T00:00:00.000Z")).toEqual(new Date("2021-08-01T00:00:00.000Z"));
    expect(isoStringToDate("2021-08-01T00:00:00.000-07:00")).toEqual(new Date("2021-08-01T07:00:00.000Z"));
    expect(isoStringToDate("2021-08-01T00:00:00.000+07:00")).toEqual(new Date("2021-07-31T17:00:00.000Z"));
    // Git >= 2.45 `%aI`: no fractional seconds, and `Z` when the offset is zero.
    expect(isoStringToDate("2016-06-15T14:13:20Z")).toEqual(new Date("2016-06-15T14:13:20Z"));
    expect(isoStringToDate("2016-06-15T16:13:20+02:00")).toEqual(new Date("2016-06-15T14:13:20Z"));
  });
});
