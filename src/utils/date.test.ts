import { isoStringToDate } from "./date";

describe("date utils: iso string", () => {
  it("should parse an iso string with timezones properly", () => {
    expect(isoStringToDate("2021-08-01T00:00:00.000Z")).toEqual(new Date("2021-08-01T00:00:00.000Z"));
    expect(isoStringToDate("2021-08-01T00:00:00.000-07:00")).toEqual(new Date("2021-08-01T07:00:00.000Z"));
    expect(isoStringToDate("2021-08-01T00:00:00.000+07:00")).toEqual(new Date("2021-07-31T17:00:00.000Z"));
  });
});
