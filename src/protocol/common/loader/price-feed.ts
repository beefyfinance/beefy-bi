import { keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { BATCH_DB_INSERT_SIZE, BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/rxjs/utils/programmer-error";

const logger = rootLogger.child({ module: "price-feed", component: "loader" });

export interface DbPriceFeed {
  priceFeedId: number;
  feedKey: string;
  externalId: string;
  priceFeedData: {
    is_active: boolean;
  };
}

export function upsertPriceFeed$<TInput, TRes>(options: {
  client: PoolClient;
  getFeedData: (obj: TInput) => Omit<DbPriceFeed, "priceFeedId">;
  formatOutput: (obj: TInput, feed: DbPriceFeed) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, feedData: options.getFeedData(obj) }));

      const results = await db_query<DbPriceFeed>(
        `INSERT INTO price_feed (feed_key, external_id, price_feed_data) VALUES %L
              ON CONFLICT (feed_key) 
              -- DO NOTHING -- can't use DO NOTHING because we need to return the id
              DO UPDATE SET feed_key = EXCLUDED.feed_key, external_id = EXCLUDED.external_id, price_feed_data = jsonb_merge(price_feed.price_feed_data, EXCLUDED.price_feed_data)
              RETURNING price_feed_id as "priceFeedId", feed_key as "feedKey", external_id as "externalId"`,
        [uniqBy(objAndData, (obj) => obj.feedData.feedKey).map((obj) => [obj.feedData.feedKey, obj.feedData.externalId, obj.feedData.priceFeedData])],
        options.client,
      );

      const idMap = keyBy(results, "feedKey");
      return objAndData.map((obj) => {
        const feed = idMap[obj.feedData.feedKey];
        if (!feed) {
          throw new ProgrammerError({ msg: "Upserted price feed not found", data: obj });
        }
        return options.formatOutput(obj.obj, feed);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function priceFeedList$<TKey extends string>(client: PoolClient, keyPrefix: TKey): Rx.Observable<DbPriceFeed> {
  logger.debug({ msg: "Fetching price feed from db", data: { keyPrefix } });
  return Rx.of(
    db_query<DbPriceFeed>(
      `SELECT 
        price_feed_id as "priceFeedId",
        feed_key as "feedKey",
        external_id as "externalId",
        price_feed_data as "priceFeedData"
      FROM price_feed 
      WHERE feed_key like %L || ':%'`,
      [keyPrefix],
      client,
    ).then((objs) => (objs.length > 0 ? objs : Promise.reject("No price feed found"))),
  ).pipe(
    Rx.mergeAll(),

    Rx.tap((priceFeeds) => logger.debug({ msg: "emitting price feed list", data: { count: priceFeeds.length } })),

    Rx.concatMap((priceFeeds) => Rx.from(priceFeeds)), // flatten
  );
}

export function fetchDbPriceFeed$<TObj, TRes>(options: {
  client: PoolClient;
  getPriceFeedId: (obj: TObj) => number;
  formatOutput: (obj: TObj, feed: DbPriceFeed | null) => TRes;
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

      const results = await db_query<DbPriceFeed>(
        `SELECT 
          price_feed_id as "priceFeedId",
          feed_key as "feedKey",
          external_id as "externalId",
          priceFeedData as "priceFeedData"
        FROM price_feed 
        WHERE price_feed_id IN (%L)`,
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
