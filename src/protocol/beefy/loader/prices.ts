import Decimal from "decimal.js";
import { keyBy, sortBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { addHistoricalDateQuery$ } from "../../common/connector/import-queries";
import { addMissingImportState$, DbOraclePriceImportState, updateImportState$ } from "../../common/loader/import-state";
import { DbPriceFeed } from "../../common/loader/price-feed";
import { upsertPrice$ } from "../../common/loader/prices";
import { ImportQuery, ImportResult } from "../../common/types/import-query";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";
import { memoryBackpressure$ } from "../../common/utils/memory-backpressure";
import { fetchBeefyPrices } from "../connector/prices";

export function importBeefyUnderlyingPrices$(options: { client: PoolClient }) {
  const logger = rootLogger.child({ module: "beefy", component: "import-underlying-prices" });

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

      // only live price feeds
      Rx.filter((priceFeed) => priceFeed.priceFeedData.active),

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

    Rx.pipe(
      // be nice with beefy api plz
      rateLimit$(300),

      // now we fetch
      Rx.concatMap(async (item) => {
        const debugLogData = {
          priceFeedKey: item.target.feedKey,
          priceFeed: item.target.priceFeedId,
          range: item.range,
        };

        logger.debug({ msg: "fetching prices", data: debugLogData });
        try {
          const prices = await fetchBeefyPrices("15min", item.target.priceFeedData.externalId, {
            startDate: item.range.from,
            endDate: item.range.to,
          });
          logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

          return Rx.of({ ...item, prices });
        } catch (error) {
          logger.error({ msg: "error fetching prices", data: { ...debugLogData, error } });
          logger.error(error);
          emitErrors(item);
          return Rx.EMPTY;
        }
      }),
      Rx.concatAll(),

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
        Rx.map((item) => ({ ...item, success: true })),
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
            (import_data->>'contractCreationDate')::timestamptz as "contractCreationDate"
        FROM import_state i
          JOIN product p on p.product_id = (i.import_data->'productId')::integer
        WHERE price_feed_2_id IN (%L)`,
        [objAndData.map((obj) => obj.priceFeedId)],
        options.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(
        results.map((res) => {
          res.contractCreationDate = new Date(res.contractCreationDate);
          return res;
        }),
        "priceFeedId",
      );
      return objAndData.map((obj) => options.formatOutput(obj.obj, idMap[obj.priceFeedId] ?? null));
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}
