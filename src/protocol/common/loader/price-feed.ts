import { keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

export interface DbAssetPriceFeed {
  assetPriceFeedId: number;
  feedKey: string;
  externalId: string;
}

export function upsertPriceFeed<TInput, TRes>(options: {
  client: PoolClient;
  getFeedData: (obj: TInput) => Omit<DbAssetPriceFeed, "assetPriceFeedId">;
  formatOutput: (obj: TInput, feed: DbAssetPriceFeed) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    // batch queries
    Rx.bufferCount(500),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, feedData: options.getFeedData(obj) }));

      const results = await db_query<DbAssetPriceFeed>(
        `INSERT INTO asset_price_feed (feed_key, external_id) VALUES %L
              ON CONFLICT (feed_key) 
              -- DO NOTHING -- can't use DO NOTHING because we need to return the id
              DO UPDATE SET feed_key = EXCLUDED.feed_key, external_id = EXCLUDED.external_id
              RETURNING asset_price_feed_id as "assetPriceFeedId", feed_key as "feedKey", external_id as "externalId"`,
        [
          uniqBy(objAndData, (obj) => obj.feedData.feedKey).map((obj) => [
            obj.feedData.feedKey,
            obj.feedData.externalId,
          ]),
        ],
        options.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "feedKey");
      return objAndData.map((obj) => options.formatOutput(obj.obj, idMap[obj.feedData.feedKey]));
    }),

    // flatten objects
    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}

export function fetchDbPriceFeed<TObj, TRes>(options: {
  client: PoolClient;
  getId: (obj: TObj) => number;
  formatOutput: (obj: TObj, feed: DbAssetPriceFeed) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchQueryGroup({
    bufferCount: 500,
    toQueryObj: (obj: TObj[]) => options.getId(obj[0]),
    getBatchKey: (obj: TObj) => options.getId(obj),
    processBatch: async (ids: number[]) => {
      const results = await db_query<DbAssetPriceFeed>(
        `SELECT 
          asset_price_feed_id as "assetPriceFeedId",
          feed_key as "feedKey",
          external_id as "externalId"
        FROM asset_price_feed 
        WHERE asset_price_feed_id IN (%L)`,
        [ids],
        options.client,
      );
      // ensure results are in the same order as the params
      const idMap = keyBy(results, (r) => r.assetPriceFeedId);
      return ids.map((id) => idMap[id]);
    },
    formatOutput: options.formatOutput,
  });
}
