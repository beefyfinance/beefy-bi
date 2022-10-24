import Decimal from "decimal.js";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { addHistoricalDateQuery$, addLatestDateQuery$ } from "../../../common/connector/import-queries";
import { fetchPriceFeedContractCreationInfos } from "../../../common/loader/fetch-product-creation-infos";
import { DbOraclePriceImportState } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../../../common/types/import-query";
import { createHistoricalImportPipeline, createRecentImportPipeline } from "../../../common/utils/historical-recent-pipeline";
import { fetchBeefyDataPrices$, PriceSnapshot } from "../../connector/prices";

const getImportStateKey = (priceFeed: DbPriceFeed) => `price:feed:${priceFeed.priceFeedId}`;

export function importBeefyHistoricalUnderlyingPrices$(options: { client: PoolClient }) {
  return createHistoricalImportPipeline<DbPriceFeed, Date, DbOraclePriceImportState>({
    client: options.client,
    chain: "bsc", // unused
    logInfos: { msg: "Importing historical underlying prices" },
    getImportStateKey,
    isLiveItem: (target) => target.priceFeedData.active,
    generateQueries$: (ctx) =>
      addHistoricalDateQuery$({
        getImport: (item) => item.importState,
        getFirstDate: (importState) => importState.importData.firstDate,
        formatOutput: (item, latestDate, queries) => queries.map((range) => ({ ...item, latest: latestDate, range })),
      }),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        // initialize the import state

        // find the first date we are interested in this price
        // so we need the first creation date of each product
        fetchPriceFeedContractCreationInfos({
          ctx: {
            ...ctx,
            emitErrors: (item) => {
              throw new Error("Error while fetching product creation infos for price feed" + item.priceFeedId);
            },
          },
          importStateType: "oracle:price",
          which: "price-feed-2", // we work on the second applied price
          getPriceFeedId: (item) => item.priceFeedId,
          productType: "beefy:vault",
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        Rx.map((item) => ({
          type: "oracle:price",
          priceFeedId: item.priceFeedId,
          firstDate: item.contractCreationInfo.contractCreationDate,
          ranges: {
            lastImportDate: new Date(),
            coveredRanges: [],
            toRetry: [],
          },
        })),
      ),
    processImportQuery$: (ctx) => insertPricePipeline$({ ctx }),
  });
}

export function importBeefyRecentUnderlyingPrices$(options: { client: PoolClient }) {
  return createRecentImportPipeline<DbPriceFeed, Date>({
    client: options.client,
    chain: "bsc", // unused
    cacheKey: "beefy:underlying:prices:recent",
    logInfos: { msg: "Importing beefy recent underlying prices" },
    getImportStateKey,
    isLiveItem: (target) => target.priceFeedData.active,
    generateQueries$: (ctx, lastImported) =>
      addLatestDateQuery$({
        getLastImportedDate: () => lastImported,
        formatOutput: (item, latestDate, query) => [{ ...item, latest: latestDate, range: query }],
      }),
    processImportQuery$: (ctx) => insertPricePipeline$({ ctx }),
  });
}

function insertPricePipeline$<TObj extends ImportRangeQuery<DbPriceFeed, Date>, TCtx extends ImportCtx<TObj>>(options: {
  ctx: TCtx;
}): Rx.OperatorFunction<TObj, ImportRangeResult<DbPriceFeed, Date>> {
  const insertPrices$ = Rx.pipe(
    // fix typings
    Rx.tap((_: ImportRangeQuery<DbPriceFeed, Date> & { price: PriceSnapshot }) => {}),

    upsertPrice$({
      ctx: options.ctx as unknown as ImportCtx<ImportRangeQuery<DbPriceFeed, Date> & { price: PriceSnapshot }>,
      getPriceData: (item) => ({
        datetime: item.price.datetime,
        blockNumber: Math.floor(item.price.datetime.getTime() / 1000),
        priceFeedId: item.target.priceFeedId,
        price: new Decimal(item.price.value),
        priceData: {},
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),
  );

  return Rx.pipe(
    fetchBeefyDataPrices$({
      ctx: options.ctx,
      getPriceParams: (item) => ({
        oracleId: item.target.priceFeedData.externalId,
        samplingPeriod: "15min",
        range: item.range,
      }),
      formatOutput: (item, prices) => ({ ...item, prices }),
    }),

    // insert prices, passthrough if there is no price so we mark the range as done
    Rx.mergeMap((item) => {
      if (item.prices.length <= 0) {
        return Rx.of({ ...item, success: true });
      }

      return Rx.from(item.prices).pipe(
        Rx.map((price) => ({ ...item, price })),
        insertPrices$,
        Rx.count(),
        Rx.map((finalCount) => ({ ...item, success: item.prices.length === finalCount })),
      );
    }),
  );
}
