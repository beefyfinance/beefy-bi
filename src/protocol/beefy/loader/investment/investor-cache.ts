import Decimal from "decimal.js";
import { groupBy, keyBy, uniq, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { Nullable } from "../../../../types/ts";
import { db_query, strAddressToPgBytea } from "../../../../utils/db";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { isInRange, rangeSortedArrayExclude } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchImportState$, isOraclePriceImportState } from "../../../common/loader/import-state";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { dbBatchCall$ } from "../../../common/utils/db-batch";
import { getPriceFeedImportStateKey } from "../../utils/import-state";

const logger = rootLogger.child({ module: "common", component: "investment" });

interface DbInvestorCacheDimensions {
  investorId: number;
  productId: number;
  datetime: Date;
  blockNumber: number;
  transactionHash: string;
  // denormalized fiels
  priceFeed1Id: number;
  priceFeed2Id: number;
  pendingRewardsPriceFeedId: number | null;
}

type DbInvestorCacheChainInfos = Nullable<{
  balance: Decimal; // moo balance
  balanceDiff: Decimal; // balance - previous balance
  shareToUnderlyingPrice: Decimal; // ppfs
  underlyingBalance: Decimal; // balance * shareToUnderlyingPrice
  underlyingDiff: Decimal; // balanceDiff * shareToUnderlyingPrice
  pendingRewards: Decimal; // pending rewards
  pendingRewardsDiff: Decimal; // pendingRewards - previous pendingRewards
}>;

// usd price infos are added afterwards
type DbInvestorCacheUsdInfos = Nullable<{
  pendingRewardsToUsdPrice: Decimal; // token price
  pendingRewardsUsdBalance: Decimal; // pendingRewards * pendingRewardsToUsdPrice
  pendingRewardsUsdDiff: Decimal; // pendingRewardsDiff * pendingRewardsToUsdPrice
  underlyingToUsdPrice: Decimal; // lp price
  usdBalance: Decimal; // underlyingBalance * underlyingToUsdPrice
  usdDiff: Decimal; // underlyingDiff * underlyingToUsdPrice
}>;

export type DbInvestorCache = DbInvestorCacheDimensions & DbInvestorCacheChainInfos & DbInvestorCacheUsdInfos;

export function upsertInvestorCacheChainInfos$<
  TObj,
  TErr extends ErrorEmitter<TObj>,
  TRes,
  TParams extends DbInvestorCacheDimensions & DbInvestorCacheChainInfos,
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getInvestorCacheChainInfos: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, investment: DbInvestorCacheChainInfos) => TRes;
}) {
  return Rx.pipe(
    Rx.map((obj: TObj) => ({ obj, params: options.getInvestorCacheChainInfos(obj) })),

    // fetch the closest price if we have it so we don't have to update the cache later on
    findClosestPriceData$({
      ctx: options.ctx,
      emitError: (err, report) => options.emitError(err.obj, report),
      getParams: (item) => ({
        datetime: item.params.datetime,
        priceFeedId: item.params.priceFeed2Id,
      }),
      formatOutput: (item, priceData) => ({ ...item, priceData }),
    }),

    dbBatchCall$({
      ctx: options.ctx,
      emitError: (err, report) => options.emitError(err.obj, report),
      formatOutput: (item) => item,
      getData: (item) => ({ ...item.params, priceData: item.priceData }),
      logInfos: { msg: "upsertInvestorCache" },
      processBatch: async (objAndData) => {
        // data should come from investment_balance_ts upsert so it should be ok and not need further processing
        const result = await db_query<{ product_id: number; investor_id: number; block_number: number }>(
          `INSERT INTO beefy_investor_timeline_cache_ts (
            investor_id,
            product_id,
            datetime,
            block_number,
            transaction_hash,
            price_feed_1_id,
            price_feed_2_id,
            pending_rewards_price_feed_id,
            balance,
            balance_diff,
            share_to_underlying_price,
            underlying_balance,
            underlying_diff,
            pending_rewards,
            pending_rewards_diff,
            underlying_to_usd_price,
            usd_balance,
            usd_diff
          ) VALUES %L
            ON CONFLICT (product_id, investor_id, block_number, datetime) 
            DO UPDATE SET 
                transaction_hash = coalesce(EXCLUDED.transaction_hash, beefy_investor_timeline_cache_ts.transaction_hash),
                balance = coalesce(EXCLUDED.balance, beefy_investor_timeline_cache_ts.balance),
                balance_diff = coalesce(EXCLUDED.balance_diff, beefy_investor_timeline_cache_ts.balance_diff),
                share_to_underlying_price = coalesce(EXCLUDED.share_to_underlying_price, beefy_investor_timeline_cache_ts.share_to_underlying_price),
                underlying_to_usd_price = coalesce(EXCLUDED.underlying_to_usd_price, beefy_investor_timeline_cache_ts.underlying_to_usd_price),
                underlying_balance = coalesce(EXCLUDED.underlying_balance, beefy_investor_timeline_cache_ts.underlying_balance),
                underlying_diff = coalesce(EXCLUDED.underlying_diff, beefy_investor_timeline_cache_ts.underlying_diff),
                pending_rewards = coalesce(EXCLUDED.pending_rewards, beefy_investor_timeline_cache_ts.pending_rewards),
                pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, beefy_investor_timeline_cache_ts.pending_rewards_diff),
                usd_balance = coalesce(EXCLUDED.usd_balance, beefy_investor_timeline_cache_ts.usd_balance),
                usd_diff = coalesce(EXCLUDED.usd_diff, beefy_investor_timeline_cache_ts.usd_diff)
            RETURNING product_id, investor_id, block_number
          `,
          [
            uniqBy(objAndData, ({ data }) => `${data.productId}:${data.investorId}:${data.blockNumber}`).map(({ data }) => {
              return [
                data.investorId,
                data.productId,
                data.datetime.toISOString(),
                data.blockNumber,
                strAddressToPgBytea(data.transactionHash),
                data.priceFeed1Id,
                data.priceFeed2Id,
                data.pendingRewardsPriceFeedId,
                data.balance ? data.balance.toString() : null,
                data.balanceDiff ? data.balanceDiff.toString() : null,
                data.shareToUnderlyingPrice ? data.shareToUnderlyingPrice.toString() : null,
                data.underlyingBalance ? data.underlyingBalance.toString() : null,
                data.underlyingDiff ? data.underlyingDiff.toString() : null,
                data.pendingRewards ? data.pendingRewards.toString() : null,
                data.pendingRewardsDiff ? data.pendingRewardsDiff.toString() : null,
                data.priceData && data.priceData.price && data.underlyingBalance && data.underlyingDiff ? data.priceData.price.toString() : null,
                data.priceData && data.priceData.price && data.underlyingBalance && data.underlyingDiff
                  ? data.priceData.price.mul(data.underlyingBalance).toString()
                  : null,
                data.priceData && data.priceData.price && data.underlyingBalance && data.underlyingDiff
                  ? data.priceData.price.mul(data.underlyingDiff).toString()
                  : null,
              ];
            }),
          ],
          options.ctx.client,
        );

        // update debug data
        const idMap = keyBy(result, (result) => `${result.product_id}:${result.investor_id}:${result.block_number}`);
        return new Map(
          objAndData.map(({ data }) => {
            const key = `${data.productId}:${data.investorId}:${data.blockNumber}`;
            const result = idMap[key];
            if (!result) {
              throw new ProgrammerError({ msg: "Upserted investment cache not found", data });
            }
            return [data, data];
          }),
        );
      },
    }),
    Rx.map((item) => item.obj),
  );
}

export function findClosestPriceData$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends { datetime: Date; priceFeedId: number }>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, closestPrice: { datetime: Date; price_feed_id: number; price: Decimal } | null) => TRes;
}) {
  return Rx.pipe(
    dbBatchCall$({
      ctx: options.ctx,
      emitError: options.emitError,
      formatOutput: options.formatOutput,
      getData: options.getParams,
      logInfos: { msg: "findClosestPriceData" },
      processBatch: async (objAndData) => {
        const matchingPrices = await db_query<{ id: number; datetime: Date; price_feed_id: number; price: string }>(
          `
            select
              t.id as id,
              pr2.price_feed_id,
              last(pr2.datetime, pr2.datetime) as datetime,
              last(pr2.price, pr2.datetime) as price
            from price_ts pr2 
            join (values %L) as t(id, datetime, price_feed_id) 
            on time_bucket('15min', pr2.datetime) = time_bucket('15min', t.datetime::timestamptz) 
                and pr2.price_feed_id = t.price_feed_id::integer
            group by 1,2;
          `,
          [objAndData.map(({ data }, index) => [index, data.datetime.toISOString(), data.priceFeedId])],
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

export function addMissingInvestorCacheUsdInfos$(options: { ctx: ImportCtx }) {
  const LIMIT_BATCH_SIZE = 5000;
  const emitError: ErrorEmitter<any> = (obj, report) => {
    logger.error({ msg: "Error updating cache price", data: { obj, report } });
  };
  type PricedRowType = { product_id: number; investor_id: number; block_number: number; datetime: Date; price: Decimal };
  type UnpricedRowType = { product_id: number; investor_id: number; block_number: number; datetime: Date; price_feed_2_id: number };

  const updatePriceCache$ = Rx.pipe(
    dbBatchCall$({
      ctx: options.ctx,
      emitError,
      formatOutput: (obj) => obj,
      getData: (obj: PricedRowType) => obj,
      logInfos: { msg: "Updating price cache" },
      processBatch: async (objAndData) => {
        // batch them by investor ID so updates only modify one partition at a time
        await db_query<never>(
          `
            update beefy_investor_timeline_cache_ts
            set
              underlying_to_usd_price = to_update.price::evm_decimal_256,
              usd_balance = underlying_balance * to_update.price::evm_decimal_256,
              usd_diff = underlying_diff * to_update.price::evm_decimal_256
            from (VALUES %L) to_update(investor_id, product_id, datetime, block_number, price)
            where to_update.investor_id::integer = beefy_investor_timeline_cache_ts.investor_id
              and to_update.product_id::integer = beefy_investor_timeline_cache_ts.product_id
              and to_update.datetime::timestamp with time zone = beefy_investor_timeline_cache_ts.datetime
              and to_update.block_number::integer = beefy_investor_timeline_cache_ts.block_number;
            `,
          [objAndData.map(({ data }) => [data.investor_id, data.product_id, data.datetime.toISOString(), data.block_number, data.price.toString()])],
          options.ctx.client,
        );
        return new Map(objAndData.map(({ data }) => [data, data]));
      },
    }),
  );

  const findFirstPrice$ = Rx.pipe(
    Rx.tap((row: UnpricedRowType) => {}),
    dbBatchCall$({
      ctx: options.ctx,
      logInfos: { msg: "find first price" },
      getData: (row) => row.price_feed_2_id,
      emitError,
      processBatch: async (objAndData) => {
        const firstPrices = await db_query<{ price_feed_id: number; datetime: Date; price: string }>(
          `
            select
              pr2.price_feed_id,
              first(pr2.datetime, pr2.datetime) as datetime,
              first(pr2.price, pr2.datetime) as price
            from price_ts pr2
            where pr2.price_feed_id in (%L)
            group by 1
        `,
          [uniq(objAndData.map(({ data }) => data))],
          options.ctx.client,
        );
        const firstPricesByFeedId = keyBy(firstPrices, (row) => row.price_feed_id);

        return new Map(
          objAndData.map(({ obj, data }) => {
            const firstPrice = firstPricesByFeedId[data];
            return [data, firstPrice ? { ...firstPrice, price: new Decimal(firstPrice.price) } : null];
          }),
        );
      },
      formatOutput: (row, firstPrice) => ({ row, firstPrice }),
    }),

    // keep only rows with a price
    excludeNullFields$("firstPrice"),

    // only keep the rows where the trx datetime is before the first price
    Rx.filter(({ row, firstPrice }) => row.datetime < firstPrice.datetime),

    // set this row price
    Rx.map(({ row, firstPrice }) => ({ ...row, price: firstPrice.price })),
  );

  return Rx.pipe(
    Rx.pipe(
      // find rows with missing usd infos
      // there is indexes on null values for this table so this should be fast
      Rx.concatMap(async () => {
        return db_query<UnpricedRowType>(
          `select
              c.investor_id,
              c.product_id,
              c.datetime,
              c.block_number,
              c.price_feed_2_id
          from beefy_investor_timeline_cache_ts c
          where c.underlying_to_usd_price is null
          limit ${LIMIT_BATCH_SIZE};`,
          [],
          options.ctx.client,
        );
      }),
      Rx.concatAll(),

      // now, try to match it with an existing price
      findClosestPriceData$({
        ctx: options.ctx,
        emitError,
        getParams: (row) => ({ datetime: row.datetime, priceFeedId: row.price_feed_2_id }),
        formatOutput: (row, matchingPrice) => ({ row, matchingPrice }),
      }),

      // now we can take 2 paths either we found a price or we didn't
      Rx.connect((items$) =>
        Rx.merge(
          // if we found a price, we are done
          items$.pipe(
            excludeNullFields$("matchingPrice"),
            Rx.map((item): PricedRowType => ({ ...item.row, price: item.matchingPrice.price })),
          ),

          // if we didn't, use a heuristic
          items$.pipe(
            Rx.filter((item) => item.matchingPrice === null),
            Rx.map((item) => item.row),
            Rx.tap((row: UnpricedRowType) => {}),

            /*
             * add a heuristic to account for the delay between contract creation and the first price we can get
             * the price feed we use can start to record a few minutes after the contract is created
             * in that case, we can have a transaction that doesn't match the `time_bucket('15min', pr2.datetime) = time_bucket('15min', c.datetime)`
             * condition above, so we need to update those rows as well. It's safe to use the first price in the price series when this happens
             * only if we know that the date range between the contract creation and the trx have been successfully imported
             * for example:
             * - contract created at datetime 2021-08-01 00:01:00 -> 15min bucket is 2021-08-01 00:00:00
             * - first price recorded at datetime 2021-08-01 00:25:00 -> 15min bucket is 2021-08-01 00:15:00
             * these 2 won't be matched by the above query, but we can safely use the first price in the series
             */
            // fetch first prices a first time just to filter out rows where the trx is after the first price
            findFirstPrice$,

            // only keep those where the import is successful between the contract creation and the first price
            // this is really inefficient to fetch them one by one, but it shouldn't happen very often
            Rx.pipe(
              fetchImportState$({
                client: options.ctx.client,
                streamConfig: {
                  ...options.ctx.streamConfig,
                  // since import state is using SELECT FOR UPDATE locks, we are better off fetching them in small amounts
                  dbMaxInputTake: 100,
                },
                getImportStateKey: (row) => getPriceFeedImportStateKey({ priceFeedId: row.price_feed_2_id }),
                formatOutput: (row, importState) => ({ row, importState }),
              }),
              excludeNullFields$("importState"),
              Rx.filter(({ row, importState }) => {
                // we should have an oracle price import state here
                if (!isOraclePriceImportState(importState)) {
                  logger.error({ msg: "Unexpected import state type", data: { importState, row } });
                  throw new ProgrammerError({ msg: "Unexpected import state type", data: { importState, row } });
                }

                const successRanges = rangeSortedArrayExclude(importState.importData.ranges.coveredRanges, importState.importData.ranges.toRetry);
                const trxDatetime = row.datetime;
                return successRanges.some((successRange) => isInRange<Date>(successRange, trxDatetime));
              }),
              Rx.map(({ row }) => row),
            ),

            // now, re-fetch price because it could have changed since the first time we fetched it
            // and the time we checked the import state
            findFirstPrice$,
          ),
        ),
      ),
      updatePriceCache$,
    ),
  );
}
