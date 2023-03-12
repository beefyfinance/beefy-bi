import Decimal from "decimal.js";
import { flatten, groupBy, keyBy, uniq, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { SamplingPeriod } from "../../../types/sampling";
import { Nullable } from "../../../types/ts";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "prices" });

export interface DbPrice {
  datetime: Date;
  priceFeedId: number;
  blockNumber: number;
  price: Decimal;
}

// upsert the address of all objects and return the id in the specified field
export function upsertPrice$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends DbPrice>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPriceData: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, price: DbPrice) => TRes;
}) {
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getPriceData,
    logInfos: { msg: "upsert price" },
    processBatch: async (objAndData) => {
      // add duplicate detection in dev only
      if (process.env.NODE_ENV === "development") {
        const duplicates = Object.entries(
          groupBy(objAndData, ({ data }) => `${data.priceFeedId}-${data.blockNumber}-${data.price.toString()}`),
        ).filter(([_, v]) => v.length > 1);
        if (duplicates.length > 0) {
          logger.error({ msg: "Duplicate prices", data: duplicates });
        }
      }

      await db_query(
        `INSERT INTO price_ts (
            datetime,
            block_number,
            price_feed_id,
            price
        ) VALUES %L
            ON CONFLICT (price_feed_id, block_number, datetime) 
            DO UPDATE SET 
              price = EXCLUDED.price
        `,
        [
          uniqBy(objAndData, ({ data }) => `${data.priceFeedId}-${data.blockNumber}`).map(({ data }) => [
            data.datetime.toISOString(),
            data.blockNumber,
            data.priceFeedId,
            data.price.toString(),
          ]),
        ],
        options.ctx.client,
      );

      return new Map(objAndData.map(({ data }) => [data, data]));
    },
  });
}

export function findMatchingPriceData$<
  TObj,
  TErr extends ErrorEmitter<TObj>,
  TRes,
  TParams extends { datetime: Date; priceFeedId: number },
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  bucketSize: SamplingPeriod;
  getParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, closestPrice: DbPrice | null) => TRes;
}) {
  return Rx.pipe(
    dbBatchCall$({
      ctx: options.ctx,
      emitError: options.emitError,
      formatOutput: options.formatOutput,
      getData: options.getParams,
      logInfos: { msg: "findClosestPriceData" },
      processBatch: async (objAndData) => {
        const matchingPrices = await db_query<DbPrice & { id: number }>(
          `
            select
              t.id as id,
              pr2.price_feed_id as "priceFeedId",
              last(pr2.block_number, pr2.datetime) as "blockNumber",
              last(pr2.datetime, pr2.datetime) as datetime,
              last(pr2.price, pr2.datetime) as price
            from price_ts pr2 
            join (values %L) as t(id, datetime, price_feed_id) 
            on time_bucket(%L, pr2.datetime) = time_bucket(%L, t.datetime::timestamptz) 
                and pr2.price_feed_id = t.price_feed_id::integer
            group by 1,2;
          `,
          [objAndData.map(({ data }, index) => [index, data.datetime.toISOString(), data.priceFeedId]), options.bucketSize, options.bucketSize],
          options.ctx.client,
        );
        const matchingPricesByInputIndex = keyBy(matchingPrices, (row) => row.id);
        return new Map(
          objAndData.map(({ obj, data }, index) => {
            const matchingPrice = matchingPricesByInputIndex[index] || null;
            return [data, matchingPrice ? { ...matchingPrice, price: new Decimal(matchingPrice.price) } : null];
          }),
        );
      },
    }),
  );
}

export function findFirstPriceData$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends { priceFeedId: number }>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, firstPrice: DbPrice | null) => TRes;
}) {
  return Rx.pipe(
    dbBatchCall$({
      ctx: options.ctx,
      logInfos: { msg: "find first price" },
      getData: (row) => options.getParams(row).priceFeedId,
      emitError: options.emitError,
      processBatch: async (objAndData) => {
        const firstPrices = await db_query<DbPrice>(
          `
          select
            price_feed_id as "priceFeedId",
            first(block_number, datetime) as "blockNumber",
            first(datetime, datetime) as "datetime",
            first(price, datetime) as "price"
          from price_ts
          where price_feed_id in (%L)
          group by 1
      `,
          [uniq(objAndData.map(({ data }) => data))],
          options.ctx.client,
        );
        const firstPricesByFeedId = keyBy(firstPrices, (row) => row.priceFeedId);

        return new Map(
          objAndData.map(({ obj, data }) => {
            const firstPrice = firstPricesByFeedId[data];
            return [data, firstPrice ? { ...firstPrice, price: new Decimal(firstPrice.price) } : null];
          }),
        );
      },
      formatOutput: options.formatOutput,
    }),
  );
}

export function interpolatePrice$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends { priceFeedId: number; datetime: Date }>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  windowSize: SamplingPeriod;
  bucketSize: SamplingPeriod;
  getQueryParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, interpolatedPrice: DbPrice | null) => TRes;
}) {
  return Rx.pipe(
    Rx.map((obj: TObj) => ({ obj, params: options.getQueryParams(obj) })),
    dbBatchCall$({
      // since we are doing unions of selects for speed, don't batch too much
      ctx: {
        ...options.ctx,
        streamConfig: {
          ...options.ctx.streamConfig,
          dbMaxInputTake: 50,
        },
      },
      logInfos: { msg: "price interpolation" },
      getData: ({ params }) => params,
      emitError: ({ obj }, report) => options.emitError(obj, report),
      processBatch: async (objAndData) => {
        const queries: string[] = [];
        const params: any[] = [];
        for (const [index, { data }] of objAndData.entries()) {
          queries.push(
            `(
                with interpolated as (
                  select
                    time_bucket_gapfill(%L::interval, p.datetime) as datetime,
                    p.price_feed_id as "priceFeedId",
                    interpolate(avg(p.block_number)) as "blockNumber",
                    interpolate(avg(p.price)) as "price"
                  from price_ts p
                  where p.price_feed_id = %L::integer
                    and datetime between %L::timestamptz - %L::interval and %L::timestamptz + %L::interval
                  group by 1, 2
                )
                select %L::integer as id, *
                from interpolated
                where datetime = time_bucket(%L::interval, %L::timestamptz)
              )
            `,
          );
          params.push([
            options.bucketSize,
            data.priceFeedId,
            data.datetime.toISOString(),
            options.windowSize,
            data.datetime.toISOString(),
            options.windowSize,
            index,
            options.bucketSize,
            data.datetime.toISOString(),
          ]);
        }

        const interpolatedPrice = await db_query<{ id: number } & Nullable<DbPrice>>(
          queries.join(" UNION ALL "),
          flatten(params),
          options.ctx.client,
        );
        const interpolatedPriceByIndex = keyBy(interpolatedPrice, (row) => row.id);

        return new Map(
          objAndData.map(({ obj, data }, index) => {
            const price = interpolatedPriceByIndex[index];
            return [
              data,
              price && price.price && price.priceFeedId && price.datetime && price.blockNumber
                ? { priceFeedId: price.priceFeedId, datetime: price.datetime, blockNumber: price.blockNumber, price: new Decimal(price.price) }
                : null,
            ];
          }),
        );
      },
      formatOutput: (item, price) => options.formatOutput(item.obj, price),
    }),
  );
}
