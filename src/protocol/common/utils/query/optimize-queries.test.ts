import deepFreeze from "deep-freeze";
import { optimizeQueries } from "./optimize-queries";
import { createOptimizerIndexFromState } from "./optimizer-index-from-state";
import { OptimizerInput, RangeIndexBuilder } from "./query-types";

describe("optimizer for range index", () => {
  const createIndex: RangeIndexBuilder<any, number> = (rangesToQuery, options) =>
    createOptimizerIndexFromState(
      rangesToQuery.map((s) => s.ranges),
      {
        mergeIfCloserThan: Math.round(options.maxRangeSize / 2),
        verticalSlicesSize: options.maxRangeSize,
      },
    );

  it("should merge ranges when possible", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 200, to: 500 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xB" }, fullRange: { from: 300, to: 700 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xC" }, fullRange: { from: 480, to: 630 }, coveredRanges: [], toRetry: [] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1000,
            maxRangeSize: 1000,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }, { key: "0xC" }],
        range: { from: 200, to: 700 },
        postFilters: [
          { obj: { key: "0xA" }, filter: [{ from: 200, to: 500 }] },
          { obj: { key: "0xB" }, filter: [{ from: 300, to: 700 }] },
          { obj: { key: "0xC" }, filter: [{ from: 480, to: 630 }] },
        ],
      },
    ]);
  });

  it("should only batch products concerned by the batch in each query", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 1200, to: 1500 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xB" }, fullRange: { from: 1300, to: 1400 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xC" }, fullRange: { from: 2200, to: 2500 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xD" }, fullRange: { from: 2300, to: 2400 }, coveredRanges: [], toRetry: [] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1000,
            maxRangeSize: 1000,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      {
        type: "address batch",
        objs: [{ key: "0xC" }, { key: "0xD" }],
        range: { from: 2200, to: 2500 },
        postFilters: [
          { obj: { key: "0xC" }, filter: "no-filter" },
          { obj: { key: "0xD" }, filter: [{ from: 2300, to: 2400 }] },
        ],
      },
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }],
        range: { from: 1200, to: 1500 },
        postFilters: [
          { obj: { key: "0xA" }, filter: "no-filter" },
          { obj: { key: "0xB" }, filter: [{ from: 1300, to: 1400 }] },
        ],
      },
    ]);
  });

  it("should account for covered ranges and retry ranges", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 200, to: 500 }, coveredRanges: [{ from: 200, to: 400 }], toRetry: [{ from: 350, to: 360 }] },
            { obj: { key: "0xB" }, fullRange: { from: 300, to: 700 }, coveredRanges: [{ from: 500, to: 700 }], toRetry: [] },
            { obj: { key: "0xC" }, fullRange: { from: 480, to: 630 }, coveredRanges: [{ from: 10, to: 600 }], toRetry: [] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1000,
            maxRangeSize: 1000,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }, { key: "0xC" }],
        range: { from: 300, to: 630 },
        postFilters: [
          { obj: { key: "0xA" }, filter: [{ from: 401, to: 500 }] },
          { obj: { key: "0xB" }, filter: [{ from: 300, to: 499 }] },
          { obj: { key: "0xC" }, filter: [{ from: 601, to: 630 }] },
        ],
      },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 350, to: 360 } },
    ]);
  });

  it("should not merge ranges when that does not make sense", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 100, to: 200 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xB" }, fullRange: { from: 400, to: 500 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xC" }, fullRange: { from: 700, to: 800 }, coveredRanges: [], toRetry: [] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 10,
            maxRangeSize: 101,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xC" }, range: { from: 700, to: 800 } },
      { type: "jsonrpc batch", obj: { key: "0xB" }, range: { from: 400, to: 500 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 100, to: 200 } },
    ]);
  });

  it("should not merge ranges when address batch is too low", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 100, to: 200 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xB" }, fullRange: { from: 200, to: 300 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xC" }, fullRange: { from: 300, to: 400 }, coveredRanges: [], toRetry: [] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 1000,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 100, to: 200 } },
      { type: "jsonrpc batch", obj: { key: "0xB" }, range: { from: 200, to: 300 } },
      { type: "jsonrpc batch", obj: { key: "0xC" }, range: { from: 300, to: 400 } },
    ]);
  });

  it("should exclude ranges that are already covered if asked so", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 200, to: 500 }, coveredRanges: [{ from: 245, to: 300 }], toRetry: [{ from: 210, to: 211 }] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 100,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      // first, the most recent blocks
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 401, to: 500 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 301, to: 400 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 212, to: 244 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 200, to: 209 } },
      // after that, the retries
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 210, to: 211 } },
    ]);
  });

  it("should respect the maxRangeSize parameter with a jsonrpc batch", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 1, to: 1000 }, coveredRanges: [{ from: 900, to: 1000 }], toRetry: [{ from: 901, to: 909 }] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 10,
            maxQueriesPerProduct: 10,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 891, to: 899 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 881, to: 890 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 871, to: 880 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 861, to: 870 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 851, to: 860 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 841, to: 850 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 831, to: 840 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 821, to: 830 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 811, to: 820 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 801, to: 810 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 901, to: 909 } },
    ]);
  });

  it("should respect the maxRangeSize parameter with an address batch", () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 100, to: 500 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xB" }, fullRange: { from: 300, to: 700 }, coveredRanges: [], toRetry: [] },
            { obj: { key: "0xC" }, fullRange: { from: 480, to: 630 }, coveredRanges: [], toRetry: [] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1000,
            maxRangeSize: 250,
            maxQueriesPerProduct: 1000,
          },
        },
        createIndex,
      ),
    ).toEqual([
      {
        type: "address batch",
        objs: [{ key: "0xB" }, { key: "0xC" }],
        range: { from: 600, to: 700 },
        postFilters: [
          { obj: { key: "0xB" }, filter: "no-filter" },
          { obj: { key: "0xC" }, filter: [{ from: 600, to: 630 }] },
        ],
      },
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }, { key: "0xC" }],
        range: { from: 350, to: 599 },
        postFilters: [
          { obj: { key: "0xA" }, filter: [{ from: 350, to: 500 }] },
          { obj: { key: "0xB" }, filter: "no-filter" },
          { obj: { key: "0xC" }, filter: [{ from: 480, to: 599 }] },
        ],
      },
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }],
        range: { from: 100, to: 349 },
        postFilters: [
          { obj: { key: "0xA" }, filter: "no-filter" },
          { obj: { key: "0xB" }, filter: [{ from: 300, to: 349 }] },
        ],
      },
    ]);
  });

  it("should respect the maxQueriesPerProduct parameter with a jsonrpc batch", async () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            { obj: { key: "0xA" }, fullRange: { from: 1, to: 1000 }, coveredRanges: [{ from: 900, to: 1000 }], toRetry: [{ from: 901, to: 909 }] },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 500,
            maxQueriesPerProduct: 3,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 501, to: 899 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 1, to: 500 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 901, to: 909 } },
    ]);
  });

  it("should merge input ranges when possible", async () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            {
              obj: { key: "0xA" },
              fullRange: { from: 1, to: 1000 },
              coveredRanges: [
                { from: 900, to: 950 },
                { from: 920, to: 1000 },
              ],
              toRetry: [{ from: 901, to: 909 }],
            },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 500,
            maxQueriesPerProduct: 3,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 501, to: 899 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 1, to: 500 } },
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 901, to: 909 } },
    ]);
  });

  it("should not crash if input ranges do not make sense", async () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            {
              obj: { key: "0xA" },
              fullRange: { from: 1000, to: 1 },
              coveredRanges: [
                { from: 950, to: 900 },
                { from: 1000, to: 920 },
              ],
              toRetry: [{ from: 909, to: 901 }],
            },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 500,
            maxQueriesPerProduct: 3,
          },
        },
        createIndex,
      ),
    ).toEqual([]);
    expect(
      optimizeQueries(
        {
          objKey: () => "",
          states: [],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 1,
            maxRangeSize: 500,
            maxQueriesPerProduct: 3,
          },
        },
        createIndex,
      ),
    ).toEqual([]);
  });

  it("should handle consecutive ranges differently if that's optimal", async () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            {
              obj: { key: "0xA" },
              fullRange: { from: 1, to: 1100 },
              coveredRanges: [{ from: 500, to: 1100 }],
              toRetry: [],
            },
            {
              obj: { key: "0xB" },
              fullRange: { from: 1, to: 1100 },
              coveredRanges: [{ from: 400, to: 1000 }],
              toRetry: [],
            },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 10,
            maxRangeSize: 500,
            maxQueriesPerProduct: 3,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xB" }, range: { from: 1001, to: 1100 } },
      {
        type: "address batch",
        objs: [{ key: "0xA" }, { key: "0xB" }],
        range: { from: 1, to: 499 },
        postFilters: [
          { obj: { key: "0xA" }, filter: "no-filter" },
          { obj: { key: "0xB" }, filter: [{ from: 1, to: 399 }] },
        ],
      },
    ]);
  });

  it("should still re-import retries even if they are imported by previous queries", async () => {
    expect(
      optimizeQueries(
        {
          objKey: (obj) => obj.key,
          states: [
            {
              obj: { key: "0xA" },
              fullRange: { from: 1, to: 1000 },
              coveredRanges: [{ from: 900, to: 1000 }],
              toRetry: [{ from: 100, to: 200 }],
            },
            {
              obj: { key: "0xB" },
              fullRange: { from: 1, to: 1000 },
              coveredRanges: [{ from: 400, to: 1000 }],
              toRetry: [{ from: 100, to: 200 }],
            },
          ],
          options: {
            ignoreImportState: false,
            maxAddressesPerQuery: 10,
            maxRangeSize: 500,
            maxQueriesPerProduct: 3,
          },
        },
        createIndex,
      ),
    ).toEqual([
      { type: "jsonrpc batch", obj: { key: "0xA" }, range: { from: 501, to: 899 } },
      {
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
      // still try to re-import this as there is a good change this failed for a technical reason
      {
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
    ]);
  });

  it("should not update any input object", async () => {
    type T = OptimizerInput<{ key: string }, number>;
    const input: T = {
      objKey: (obj) => obj.key,
      states: [
        {
          obj: { key: "0xA" },
          fullRange: { from: 1, to: 1000 },
          coveredRanges: [
            { from: 900, to: 950 },
            { from: 920, to: 1000 },
          ],
          toRetry: [{ from: 901, to: 909 }],
        },
      ],
      options: {
        ignoreImportState: false,
        maxAddressesPerQuery: 1,
        maxRangeSize: 500,
        maxQueriesPerProduct: 3,
      },
    };
    optimizeQueries(deepFreeze(input) as unknown as T, createIndex);
  });
});
