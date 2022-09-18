import { keyBy, omit, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query } from "../../../utils/db";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

export interface DbAssetPriceFeed {
  assetPriceFeedId: number;
  feedKey: string;
  externalId: string;
}

export function upsertPriceFeed<
  TObj extends {
    assetPriceFeed: {
      feedKey: string;
      externalId: string;
    };
  },
>(client: PoolClient): Rx.OperatorFunction<TObj, Omit<TObj, "assetPriceFeed"> & { assetPriceFeedId: number }> {
  return Rx.pipe(
    // batch queries
    Rx.bufferCount(500),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      type TRes = { asset_price_feed_id: number; feed_key: string };
      const results = await db_query<TRes>(
        `INSERT INTO asset_price_feed (feed_key, external_id) VALUES %L
              ON CONFLICT (feed_key) 
              -- DO NOTHING -- can't use DO NOTHING because we need to return the id
              DO UPDATE SET feed_key = EXCLUDED.feed_key, external_id = EXCLUDED.external_id
              RETURNING asset_price_feed_id, feed_key`,
        [
          uniqBy(objs, (obj) => obj.assetPriceFeed.feedKey).map((obj) => [
            obj.assetPriceFeed.feedKey,
            obj.assetPriceFeed.externalId,
          ]),
        ],
        client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "feed_key");
      return objs.map((obj) => {
        return omit(
          { ...obj, assetPriceFeedId: idMap[obj.assetPriceFeed.feedKey].asset_price_feed_id },
          "assetPriceFeed",
        );
      });
    }),

    // flatten objects
    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}

export function mapPriceFeed<TObj, TKey extends string>(
  client: PoolClient,
  getId: (obj: TObj) => number,
  toKey: TKey,
): Rx.OperatorFunction<TObj, TObj & { [key in TKey]: DbAssetPriceFeed }> {
  const toQueryObj = (obj: TObj[]) => getId(obj[0]);
  const process = async (ids: number[]) => {
    const results = await db_query<DbAssetPriceFeed>(
      `SELECT 
        asset_price_feed_id as "assetPriceFeedId",
        feed_key as "feedKey",
        external_id as "externalId"
      FROM asset_price_feed 
      WHERE asset_price_feed_id IN (%L)`,
      [ids],
      client,
    );
    // ensure results are in the same order as the params
    const idMap = keyBy(results, (r) => r.assetPriceFeedId);
    return ids.map((id) => idMap[id]);
  };

  return Rx.pipe(
    // betch queries
    Rx.bufferCount(500),

    batchQueryGroup(toQueryObj, getId, process, toKey),

    // flatten objects
    Rx.mergeMap((objs) => Rx.from(objs)),
  );
}
