import Decimal from "decimal.js";
import * as Rx from "rxjs";
import { DbClient } from "../../../../utils/db";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { addHistoricalDateQuery$, addLatestDateQuery$ } from "../../../common/connector/import-queries";
import { fetchPriceFeedContractCreationInfos } from "../../../common/loader/fetch-product-creation-infos";
import { DbOraclePriceImportState } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../../../common/types/import-query";
import { executeSubPipeline$ } from "../../../common/utils/execute-sub-pipeline";
import { createHistoricalImportPipeline, createRecentImportPipeline } from "../../../common/utils/historical-recent-pipeline";
import { fetchBeefyDataPrices$ } from "../../connector/prices";

const getImportStateKey = (priceFeed: DbPriceFeed) => `price:feed:${priceFeed.priceFeedId}`;

export function importBeefyHistoricalUnderlyingPrices$(options: { client: DbClient }) {
  return createHistoricalImportPipeline<DbPriceFeed, Date, DbOraclePriceImportState>({
    client: options.client,
    rpcCount: 1, // unused
    chain: "bsc", // unused
    logInfos: { msg: "Importing historical underlying prices" },
    getImportStateKey,
    isLiveItem: (target) => target.priceFeedData.active,
    generateQueries$: (ctx) =>
      Rx.pipe(
        addHistoricalDateQuery$({
          getImport: (item) => item.importState,
          getFirstDate: (importState) => importState.importData.firstDate,
          formatOutput: (item, latestDate, queries) => queries.map((range) => ({ ...item, latest: latestDate, range })),
        }),
        Rx.concatAll(),
      ),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        Rx.map((obj) => ({ obj })),
        // initialize the import state

        // find the first date we are interested in this price
        // so we need the first creation date of each product
        fetchPriceFeedContractCreationInfos({
          ctx,
          emitError: (item) => {
            throw new Error("Error while fetching product creation infos for price feed" + item.obj.priceFeedId);
          },
          importStateType: "product:investment", // we want to find the contract creation date we already fetched from the investment pipeline
          which: "price-feed-2", // we are the feed 2 from the product
          getPriceFeedId: (item) => item.obj.priceFeedId,
          productType: "beefy:vault",
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        Rx.map((item) => ({
          obj: item.obj,
          importData: {
            type: "oracle:price",
            priceFeedId: item.obj.priceFeedId,
            firstDate: item.contractCreationInfo.contractCreationDate,
            ranges: {
              lastImportDate: new Date(),
              coveredRanges: [],
              toRetry: [],
            },
          },
        })),
      ),
    processImportQuery$: (ctx, emitError) => insertPricePipeline$({ ctx, emitError }),
  });
}

export function importBeefyRecentUnderlyingPrices$(options: { client: DbClient }) {
  return createRecentImportPipeline<DbPriceFeed, Date>({
    client: options.client,
    rpcCount: 1, // unused
    chain: "bsc", // unused
    cacheKey: "beefy:underlying:prices:recent",
    logInfos: { msg: "Importing beefy recent underlying prices" },
    getImportStateKey,
    isLiveItem: (target) => target.priceFeedData.active,
    generateQueries$: ({ ctx, emitError, lastImported, formatOutput }) =>
      addLatestDateQuery$({
        getLastImportedDate: () => lastImported,
        formatOutput: (item, latestDate, query) => formatOutput(item, latestDate, [query]),
      }),
    processImportQuery$: (ctx, emitError) => insertPricePipeline$({ ctx, emitError }),
  });
}

function insertPricePipeline$<TObj extends ImportRangeQuery<DbPriceFeed, Date>, TErr extends ErrorEmitter<TObj>>(options: {
  ctx: ImportCtx;
  emitError: TErr;
}) {
  return Rx.pipe(
    Rx.tap((_: TObj) => {}),

    fetchBeefyDataPrices$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPriceParams: (item) => ({
        oracleId: item.target.priceFeedData.externalId,
        samplingPeriod: "15min",
        range: item.range,
      }),
      formatOutput: (item, prices) => ({ ...item, prices }),
    }),

    executeSubPipeline$({
      ctx: options.ctx,
      emitError: options.emitError,
      getObjs: (item) => item.prices,
      pipeline: (emitError) =>
        Rx.pipe(
          upsertPrice$({
            ctx: options.ctx,
            emitError,
            getPriceData: (item) => ({
              datetime: item.target.datetime,
              blockNumber: Math.floor(item.target.datetime.getTime() / 1000),
              priceFeedId: item.parent.target.priceFeedId,
              price: new Decimal(item.target.value),
              priceData: {},
            }),
            formatOutput: (priceData, price) => ({ ...priceData, result: price }),
          }),
        ),
      formatOutput: (item, prices) => ({ ...item, prices, success: true }),
    }),

    // format output
    Rx.map(
      (item): ImportRangeResult<DbPriceFeed, Date> => ({
        target: item.target,
        latest: item.latest,
        range: item.range,
        success: item.success,
      }),
    ),
  );
}
