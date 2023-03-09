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
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    formatOutput: options.formatOutput,
    getData: options.getInvestorCacheChainInfos,
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
            pending_rewards_diff
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
                pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, beefy_investor_timeline_cache_ts.pending_rewards_diff)
            RETURNING product_id, investor_id, block_number
          `,
        [
          uniqBy(objAndData, ({ data }) => `${data.productId}:${data.investorId}:${data.blockNumber}`).map(({ data }) => [
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
          ]),
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
  });
}

export function addMissingInvestorCacheUsdInfos$(options: { ctx: ImportCtx }) {
  const LIMIT_BATCH_SIZE = 5000;
  const emitError: ErrorEmitter<any> = (obj, report) => {
    logger.error({ msg: "Error updating cache price", data: { obj, report } });
  };
  type PricedRowType = { product_id: number; investor_id: number; block_number: number; datetime: Date; price: string };
  type UnpricedRowType = { product_id: number; investor_id: number; block_number: number; datetime: Date; price_feed_2_id: number };

  const updatePriceCache$ = Rx.pipe(
    Rx.tap((rows: PricedRowType[]) => {}),
    Rx.concatMap(async (rows) => {
      logger.debug({ msg: "Updating price cache", data: { rowCount: rows.length } });
      // batch them by investor ID so updates only modify one partition at a time
      const rowsByInvestor = groupBy(rows, (row) => `${row.investor_id}`);

      // do the actual update, it's better to use this when all rows hit the same partition
      for (const [investor, rows] of Object.entries(rowsByInvestor)) {
        logger.debug({ msg: "Updating price cache batch", data: { investor, rowCount: rows.length } });
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
          [rows.map((row) => [row.investor_id, row.product_id, row.datetime.toISOString(), row.block_number, row.price])],
          options.ctx.client,
        );
      }

      return rows;
    }),
    Rx.toArray(),
    Rx.map((rows) => rows.flat()),
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
            return [data, firstPrice ?? null];
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
      Rx.concatMap(async () => {
        // find rows with missing usd infos where we have a price
        // the price feed has a point every 15min so we try to match the prices using `time_bucket('15min', pr2.datetime) = time_bucket('15min', c.datetime)`
        return db_query<PricedRowType>(
          ` 
          select
            c.investor_id,
            c.product_id,
            c.datetime,
            c.block_number,
            last(pr2.price, pr2.datetime) as price
          from beefy_investor_timeline_cache_ts c
            join price_ts pr2 on c.price_feed_2_id = pr2.price_feed_id
            and time_bucket('15min', pr2.datetime) = time_bucket('15min', c.datetime)
          where c.underlying_to_usd_price is null
          group by 1,2,3,4
          limit ${LIMIT_BATCH_SIZE} -- limit to avoid locking the table for too long
      `,
          [],
          options.ctx.client,
        );
      }),
      // update investor rows
      updatePriceCache$,
    ),
    Rx.pipe(
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
      Rx.pipe(
        Rx.concatMap(async (updatedRows) => {
          // skip this heuristic if we think we still have missing prices to update
          if (updatedRows.length >= LIMIT_BATCH_SIZE) {
            return [];
          }
          // find rows with missing usd infos where we don't have a price
          const missingUsdRows = await db_query<UnpricedRowType>(
            ` 
            select
              c.investor_id,
              c.product_id,
              c.datetime,
              c.block_number,
              c.price_feed_2_id
            from beefy_investor_timeline_cache_ts c
            where c.underlying_to_usd_price is null
            limit ${LIMIT_BATCH_SIZE} -- limit to avoid locking the table for too long
        `,
            [],
            options.ctx.client,
          );

          if (missingUsdRows.length === 0) {
            return [];
          }
          logger.debug({ msg: "found rows with missing usd infos where we don't have a matching price", data: { rowCount: missingUsdRows.length } });
          logger.trace({ msg: "found rows with missing usd infos where we don't have a matching price", data: { missingUsdRows } });
          return missingUsdRows;
        }),
        Rx.concatAll(),
      ),

      Rx.pipe(
        // fetch first prices a first time just to filter out rows where the trx is after the first price
        findFirstPrice$,
      ),

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
            return false;
          }

          const successRanges = rangeSortedArrayExclude(importState.importData.ranges.coveredRanges, importState.importData.ranges.toRetry);
          const trxDatetime = row.datetime;
          return successRanges.some((successRange) => isInRange<Date>(successRange, trxDatetime));
        }),
        Rx.map(({ row }) => row),
      ),

      Rx.pipe(
        // now, re-fetch price because it could have changed since the first fetch
        findFirstPrice$,

        // update investor rows with the first prices
        Rx.toArray(),
        updatePriceCache$,
      ),
    ),
  );
}
