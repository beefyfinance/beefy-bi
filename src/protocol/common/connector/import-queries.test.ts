import { _restrictRangesWithImportState } from "./import-queries";
describe("import-queries", () => {
  it("should restrict import ranges using import state", async () => {
    const ranges = [{ from: 1, to: 1000 }];
    const coveredRanges = [{ from: 900, to: 1000 }];
    const toRetry = [{ from: 901, to: 909 }];
    const maxRangeLength = 100;
    const limitRangeCount = 10;

    const importState: any = { importData: { ranges: { coveredRanges, toRetry } } };
    const res = _restrictRangesWithImportState(ranges, importState, maxRangeLength, limitRangeCount);
    expect(res).toEqual([
      // first, try to import new data
      { from: 800, to: 899 },
      { from: 700, to: 799 },
      { from: 600, to: 699 },
      { from: 500, to: 599 },
      { from: 400, to: 499 },
      { from: 300, to: 399 },
      { from: 200, to: 299 },
      { from: 100, to: 199 },
      { from: 1, to: 99 },
      // then, retry failed imports
      { from: 901, to: 909 },
    ]);
  });

  it("should respect the maxRangeLength parameter", async () => {
    const ranges = [{ from: 1, to: 1000 }];
    const coveredRanges = [{ from: 900, to: 1000 }];
    const toRetry = [{ from: 901, to: 909 }];
    const maxRangeLength = 10;
    const limitRangeCount = 10;

    const importState: any = { importData: { ranges: { coveredRanges, toRetry } } };
    const res = _restrictRangesWithImportState(ranges, importState, maxRangeLength, limitRangeCount);
    expect(res).toEqual([
      { from: 890, to: 899 },
      { from: 880, to: 889 },
      { from: 870, to: 879 },
      { from: 860, to: 869 },
      { from: 850, to: 859 },
      { from: 840, to: 849 },
      { from: 830, to: 839 },
      { from: 820, to: 829 },
      { from: 810, to: 819 },
      { from: 800, to: 809 },
    ]);
  });

  it("should respect the limitRangeCount parameter", async () => {
    const ranges = [{ from: 1, to: 1000 }];
    const coveredRanges = [{ from: 900, to: 1000 }];
    const toRetry = [{ from: 901, to: 909 }];
    const maxRangeLength = 500;
    const limitRangeCount = 3;

    const importState: any = { importData: { ranges: { coveredRanges, toRetry } } };
    const res = _restrictRangesWithImportState(ranges, importState, maxRangeLength, limitRangeCount);
    expect(res).toEqual([
      { from: 400, to: 899 },
      { from: 1, to: 399 },
      { from: 901, to: 909 },
    ]);
  });

  it("should merge input ranges when possible", async () => {
    const ranges = [
      { from: 1, to: 500 },
      { from: 501, to: 1000 },
    ];
    const coveredRanges = [
      { from: 900, to: 950 },
      { from: 920, to: 1000 },
    ];
    const toRetry = [{ from: 901, to: 909 }];
    const maxRangeLength = 500;
    const limitRangeCount = 3;

    const importState: any = { importData: { ranges: { coveredRanges, toRetry } } };
    const res = _restrictRangesWithImportState(ranges, importState, maxRangeLength, limitRangeCount);
    expect(res).toEqual([
      { from: 501, to: 899 },
      { from: 1, to: 500 },
      { from: 901, to: 909 },
    ]);
  });
});
