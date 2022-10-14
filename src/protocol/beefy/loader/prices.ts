import Decimal from "decimal.js";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { rootLogger } from "../../../utils/logger";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { DbPriceFeed } from "../../common/loader/price-feed";
import { findMissingPriceRangeInDb$, upsertPrice$ } from "../../common/loader/prices";
import { fetchBeefyPrices } from "../connector/prices";

export function importBeefyUnderlyingPrices$(options: { client: PoolClient }) {
  const logger = rootLogger.child({ module: "beefy", component: "import-underlying-prices" });
  /*
  const rpcConfig = createRpcConfig(chain);

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
  const { observable: productErrors$, next: emitErrors } = createObservableWithNext<ImportQuery<DbProduct>>();

  const getImportStateKey = (priceFeedId: number) => `price:feed:${priceFeedId}`;
*/
  return Rx.pipe(
    Rx.pipe(
      Rx.tap((priceFeed: DbPriceFeed) => logger.debug({ msg: "fetching beefy underlying prices", data: priceFeed })),

      // only live price feeds
      Rx.filter((priceFeed) => priceFeed.priceFeedData.active),

      // map to an object where we can add attributes to safely
      Rx.map((priceFeed) => ({ priceFeed })),

      // remove duplicates
      Rx.distinct(({ priceFeed }) => priceFeed.priceFeedData.externalId),
    ),

    // find which data is missing
    findMissingPriceRangeInDb$({
      client: options.client,
      getFeedId: (productData) => productData.priceFeed.priceFeedId,
      formatOutput: (productData, missingData) => ({ ...productData, missingData }),
    }),
    /*
    addMissingImportState$({
      client: options.client,
      getImportStateKey: (item) => getImportStateKey(item.priceFeed.priceFeedId),
      addDefaultImportData$: (formatOutput) =>
        Rx.pipe(
          // initialize the import state

          // find the first date we are interested in this price
          // so we need the first creation date of each product
          fetchProductContractCreationInfos({
            client: options.client,
            getPriceFeedId: (item) => item.priceFeed.priceFeedId,
            formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
          }),

          // drop those without a creation info
          excludeNullFields$("contractCreationInfo"),

          Rx.map((item) =>
            formatOutput(item, {
              type: "oracle:price",
              priceFeedId: item.priceFeedId,
              firstDate: item.contractCreationInfo.contractCreationDate,
              ranges: {
                lastImportDate: new Date(),
                coveredRanges: [],
                toRetry: [],
              },
            }),
          ),
        ),
      formatOutput: (product, importState) => ({ target: product, importState }),
    }),

    // generate the queries
    addHistoricalDateQuery$({
      getImport: (item) => item.importState;
      getFirstDate: (item) => item.importState.importData.firstDate,
      formatOutput: (item, latestDate, queries) => ({ ...item, latestDate, queries }),
    }),

    // convert to stream of price queries
    Rx.concatMap((item) =>
      item.queries.map((range) => {
        const { queries, ...rest } = item;
        return { ...rest, range, latest: item.latest };
      }),
    ),

    // some backpressure mechanism
    Rx.pipe(
      memoryBackpressure$({
        logInfos: { msg: "import-price-data", data: { chain } },
        sendBurstsOf: streamConfig.maxInputTake,
      }),

      Rx.tap((item) =>
        logger.info({
          msg: "processing price query",
          data: { chain: item.target.chain, productId: item.target.productId, product_key: item.target.productKey, range: item.range },
        }),
      ),
    ),


    Rx.pipe(
      // be nice with beefy api plz
      rateLimit$(300),

      // now we fetch
      Rx.mergeMap(async (item) => {
        const debugLogData = {
          priceFeedKey: item.priceFeed.feedKey,
          priceFeed: item.priceFeed.priceFeedId,
          range: item.range,
        };

        logger.debug({ msg: "fetching prices", data: debugLogData });
        const prices = await fetchBeefyPrices("15min", item.priceFeed.priceFeedData.externalId, {
          startDate: item.range.from,
          endDate: item.range.to,
        });
        logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

        return { ...item, prices };
      }, 
      // force concurrency to 1 to avoid rate limit issues
      1 
      ),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.concatMap((productData) => Rx.from(productData.prices.map((price) => ({ ...productData, price })))),
    ),

    upsertPrice$({
      client: options.client,
      getPriceData: (item) => ({
        datetime: item.price.datetime,
        blockNumber: Math.floor(item.price.datetime.getTime() / 1000),
        priceFeedId: item.priceFeed.priceFeedId,
        price: new Decimal(item.price.value),
        priceData: {},
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),

    // handle the results
    Rx.pipe(
      Rx.map((item) => ({ ...item, success: true })),
      // make sure we close the errors observable when we are done
      Rx.finalize(() => setTimeout(completePriceErrors$, 1000)),
      // merge the errors back in, all items here should have been successfully treated
      Rx.mergeWith(priceErrors$.pipe(Rx.map((item) => ({ ...item, success: false })))),
      // make sure the type is correct
      //Rx.map((item): ImportResult<DbBeefyProduct, number> => item),
    ),

    // update the import state
    updateImportState$({ client, streamConfig, getImportStateKey: (item) => getImportStateKey(item.priceFeed.priceFeedId), formatOutput: (item) => item }),
*/
    Rx.pipe(
      // be nice with beefy api plz
      rateLimit$(300),

      // now we fetch
      Rx.mergeMap(async (item) => {
        const debugLogData = {
          priceFeedKey: item.priceFeed.feedKey,
          priceFeed: item.priceFeed.priceFeedId,
          missingData: item.missingData,
        };

        logger.debug({ msg: "fetching prices", data: debugLogData });
        const prices = await fetchBeefyPrices("15min", item.priceFeed.priceFeedData.externalId, {
          startDate: item.missingData.fromDate,
          endDate: item.missingData.toDate,
        });
        logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

        return { ...item, prices };
      }, 1 /* concurrency */),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.concatMap((productData) => Rx.from(productData.prices.map((price) => ({ ...productData, price })))),
    ),

    upsertPrice$({
      client: options.client,
      getPriceData: (item) => ({
        datetime: item.price.datetime,
        blockNumber: Math.floor(item.price.datetime.getTime() / 1000),
        priceFeedId: item.priceFeed.priceFeedId,
        price: new Decimal(item.price.value),
        priceData: {},
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),
  );
}

/*

function fetchProductContractCreationInfos<TObj, TRes>(options: {
  client: PoolClient;
  getPriceFeedId: (obj: TObj) => number;
  formatOutput: (obj: TObj, contractCreationInfos: { contractCreatedAtBlock: number; contractCreationDate: Date } | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_SELECT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, priceFeedId: options.getPriceFeedId(obj) }));

      type TRes = { priceFeedId: number; contractCreatedAtBlock: number; contractCreationDate: Date };
      const results = await db_query<TRes>(
        `SELECT 
            p.price_feed_2_id as "priceFeedId",
            (import_data->'contractCreatedAtBlock')::integer as "contractCreatedAtBlock",
            (import_data->>'contractCreationDate')::timestamptz as "contractCreationDate",
        FROM import_state i
          JOIN product p on p.product_id = (i.import_data->'productId')::integer
        WHERE price_feed_2_id IN (%L)`,
        [objAndData.map((obj) => obj.priceFeedId)],
        options.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "priceFeedId");
      return objAndData.map((obj) => options.formatOutput(obj.obj, idMap[obj.priceFeedId] ?? null));
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}
*/
