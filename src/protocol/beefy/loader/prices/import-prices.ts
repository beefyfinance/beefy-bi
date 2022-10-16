import Decimal from "decimal.js";
import { get, sortBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS } from "../../../../utils/config";
import { rootLogger } from "../../../../utils/logger";
import { createObservableWithNext } from "../../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { addHistoricalDateQuery$, addLatestDateQuery$ } from "../../../common/connector/import-queries";
import { addMissingImportState$, DbOraclePriceImportState, updateImportState$ } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { ImportQuery, ImportResult } from "../../../common/types/import-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { memoryBackpressure$ } from "../../../common/utils/memory-backpressure";
import { fetchBeefyDataPrices$ } from "../../connector/prices";
import { fetchProductContractCreationInfos } from "./fetch-product-creation-infos";

export function importBeefyHistoricalUnderlyingPrices$(options: { client: PoolClient }) {
  const logger = rootLogger.child({ module: "beefy", component: "import-historical-underlying-prices" });

  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };
  const {
    observable: priceFeedErrors$,
    complete: completePriceFeedErrors$,
    next: emitErrors,
  } = createObservableWithNext<ImportQuery<DbPriceFeed, Date>>();

  const getImportStateKey = (priceFeedId: number) => `price:feed:${priceFeedId}`;

  return Rx.pipe(
    Rx.pipe(
      Rx.tap((priceFeed: DbPriceFeed) => logger.debug({ msg: "fetching beefy underlying prices", data: priceFeed })),

      // map to an object where we can add attributes to safely
      Rx.map((priceFeed) => ({ target: priceFeed })),

      // remove duplicates
      Rx.distinct(({ target }) => target.priceFeedData.externalId),
    ),

    addMissingImportState$({
      client: options.client,
      streamConfig,
      getImportStateKey: (item) => getImportStateKey(item.target.priceFeedId),
      addDefaultImportData$: (formatOutput) =>
        Rx.pipe(
          // initialize the import state

          // find the first date we are interested in this price
          // so we need the first creation date of each product
          fetchProductContractCreationInfos({
            client: options.client,
            getPriceFeedId: (item) => item.target.priceFeedId,
            formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
          }),

          // drop those without a creation info
          excludeNullFields$("contractCreationInfo"),

          Rx.map((item) =>
            formatOutput(item, {
              type: "oracle:price",
              priceFeedId: item.target.priceFeedId,
              firstDate: item.contractCreationInfo.contractCreationDate,
              ranges: {
                lastImportDate: new Date(),
                coveredRanges: [],
                toRetry: [],
              },
            }),
          ),
        ),
      formatOutput: (item, importState) => ({ ...item, importState }),
    }),

    // fix ts types
    Rx.filter((item): item is { target: DbPriceFeed; importState: DbOraclePriceImportState } => !!item),

    // process first the prices we imported the least
    Rx.pipe(
      Rx.toArray(),
      Rx.map((items) => sortBy(items, (item) => item.importState.importData.ranges.lastImportDate)),
      Rx.concatAll(),
    ),

    Rx.pipe(
      // generate the queries
      addHistoricalDateQuery$({
        getImport: (item) => item.importState,
        getFirstDate: (importState) => importState.importData.firstDate,
        formatOutput: (item, latestDate, queries) => ({ ...item, latest: latestDate, queries }),
      }),

      // convert to stream of price queries
      Rx.concatMap((item) =>
        item.queries.map((range): ImportQuery<DbPriceFeed, Date> => {
          const { queries, ...rest } = item;
          return { ...rest, range, latest: item.latest };
        }),
      ),

      // some backpressure mechanism
      Rx.pipe(
        memoryBackpressure$({
          logInfos: { msg: "import-price-data" },
          sendBurstsOf: streamConfig.maxInputTake,
        }),

        Rx.tap((item) =>
          logger.info({
            msg: "processing price query",
            data: { feedKey: item.target.feedKey, range: item.range },
          }),
        ),
      ),
    ),

    fetchBeefyDataPrices$({
      emitErrors,
      streamConfig,
      getPriceParams: (item) => ({
        oracleId: item.target.priceFeedData.externalId,
        samplingPeriod: "15min",
      }),
      formatOutput: (item, prices) => ({ ...item, prices }),
    }),

    Rx.pipe(
      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.concatMap((item) => Rx.from(item.prices.map((price) => ({ ...item, price })))),
    ),

    upsertPrice$({
      client: options.client,
      emitErrors,
      streamConfig: streamConfig,
      getPriceData: (item) => ({
        datetime: item.price.datetime,
        blockNumber: Math.floor(item.price.datetime.getTime() / 1000),
        priceFeedId: item.target.priceFeedId,
        price: new Decimal(item.price.value),
        priceData: {},
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),

    Rx.pipe(
      // handle the results
      Rx.pipe(
        Rx.map((item) => ({ ...item, success: get(item, "success", true) })),
        // make sure we close the errors observable when we are done
        Rx.finalize(() => setTimeout(completePriceFeedErrors$, 1000)),
        // merge the errors back in, all items here should have been successfully treated
        Rx.mergeWith(priceFeedErrors$.pipe(Rx.map((item) => ({ ...item, success: false })))),
        // make sure the type is correct
        Rx.map((item): ImportResult<DbPriceFeed, Date> => item),
      ),

      // update the import state
      updateImportState$({
        client: options.client,
        streamConfig,
        getImportStateKey: (item) => getImportStateKey(item.target.priceFeedId),
        formatOutput: (item) => item,
      }),
    ),
  );
}

// remember the last imported block number for each chain so we can reduce the amount of data we fetch
let lastImportDate: Date | null = null;

export function importBeefyRecentUnderlyingPrices$(options: { client: PoolClient }) {
  const logger = rootLogger.child({ module: "beefy", component: "import-recent-underlying-prices" });

  const streamConfig: BatchStreamConfig = {
    // since we are doing live data on a small amount of queries (one per vault)
    // we can afford some amount of concurrency
    workConcurrency: 10,
    // But we can not afford to wait before processing the next batch
    maxInputWaitMs: 5_000,
    maxInputTake: 500,
    // and we cannot afford too long of a retry per product
    maxTotalRetryMs: 10_000,
  };

  return Rx.pipe(
    Rx.pipe(
      Rx.tap((priceFeed: DbPriceFeed) => logger.debug({ msg: "fetching beefy underlying prices", data: priceFeed })),

      // only live price feeds
      Rx.filter((priceFeed) => priceFeed.priceFeedData.active),

      // map to an object where we can add attributes to safely
      Rx.map((priceFeed) => ({ target: priceFeed })),

      // remove duplicates
      Rx.distinct(({ target }) => target.priceFeedData.externalId),
    ),

    // generate the query
    addLatestDateQuery$({
      getLastImportedDate: () => lastImportDate,
      formatOutput: (item, latestDate, query) => ({ ...item, latest: latestDate, range: query }),
    }),

    Rx.pipe(
      fetchBeefyDataPrices$({
        emitErrors: () => {}, // ignore errors
        streamConfig,
        getPriceParams: (item) => ({
          oracleId: item.target.priceFeedData.externalId,
          samplingPeriod: "15min",
        }),
        formatOutput: (item, prices) => ({ ...item, prices }),
      }),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.concatMap((item) => Rx.from(item.prices.map((price) => ({ ...item, price })))),
    ),

    upsertPrice$({
      client: options.client,
      emitErrors: () => {}, // ignore errors
      streamConfig: streamConfig,
      getPriceData: (item) => ({
        datetime: item.price.datetime,
        blockNumber: Math.floor(item.price.datetime.getTime() / 1000),
        priceFeedId: item.target.priceFeedId,
        price: new Decimal(item.price.value),
        priceData: {},
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),

    // logging
    Rx.tap((item) => {
      // update the local state
      if (lastImportDate === null || item.range.to > lastImportDate) {
        lastImportDate = item.range.to;
      }
    }),
  );
}
