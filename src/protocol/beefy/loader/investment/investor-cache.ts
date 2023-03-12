import Decimal from "decimal.js";
import { keyBy, uniqBy } from "lodash";
import * as Rx from "rxjs";
import { samplingPeriodMs } from "../../../../types/sampling";
import { Nullable } from "../../../../types/ts";
import { db_query, strAddressToPgBytea } from "../../../../utils/db";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { rangeInclude, rangeMerge, rangeSortedArrayExclude } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { DbDateRangeImportState, fetchImportState$, isOraclePriceImportState } from "../../../common/loader/import-state";
import { findFirstPriceData$, findMatchingPriceData$, interpolatePrice$ } from "../../../common/loader/prices";
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
    findMatchingPriceData$({
      ctx: options.ctx,
      bucketSize: "15min", // since we only have a 15min precision on the LP price data
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

export function addMissingInvestorCacheUsdInfos$(options: { ctx: ImportCtx }) {
  const LIMIT_BATCH_SIZE = 50000;
  const emitError: ErrorEmitter<any> = (obj, report) => {
    logger.error({ msg: "Error updating cache price", data: { obj, report } });
  };
  type PricedRowType = { product_id: number; investor_id: number; block_number: number; datetime: Date; price: Decimal };
  type UnpricedRowType = { product_id: number; investor_id: number; block_number: number; datetime: Date; price_feed_2_id: number };

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
          from beefy_investor_timeline_cache_ts c TABLESAMPLE BERNOULLI (10) -- sample on 1% of the rows to randomize the order
          where c.underlying_to_usd_price is null
          limit ${LIMIT_BATCH_SIZE};`,
          [],
          options.ctx.client,
        );
      }),
      Rx.concatAll(),

      // now, try to match it with an existing price
      findMatchingPriceData$({
        ctx: options.ctx,
        bucketSize: "15min",
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
            Rx.map(({ row }) => row),

            // for any heuristic we have, we'll have to check the import state, so we fetch it first
            Rx.pipe(
              fetchImportState$({
                client: options.ctx.client,
                streamConfig: {
                  ...options.ctx.streamConfig,
                  // since import state is using SELECT FOR UPDATE locks, we are better off fetching them in small amounts
                  dbMaxInputTake: 10,
                },
                getImportStateKey: (row) => getPriceFeedImportStateKey({ priceFeedId: row.price_feed_2_id }),
                formatOutput: (row, importState) => ({ row, importState }),
              }),
              excludeNullFields$("importState"),
              Rx.filter((item): item is { row: UnpricedRowType; importState: DbDateRangeImportState } => {
                // we should have an oracle price import state here
                if (!isOraclePriceImportState(item.importState)) {
                  logger.error({ msg: "Unexpected import state type", data: item });
                  throw new ProgrammerError({ msg: "Unexpected import state type", data: item });
                }
                return true;
              }),
              Rx.map((item) => ({
                ...item,
                successRanges: rangeMerge(
                  rangeSortedArrayExclude(item.importState.importData.ranges.coveredRanges, item.importState.importData.ranges.toRetry),
                ),
                contractCreation: item.importState.importData.firstDate,
              })),
            ),

            // fetch first price a first time
            findFirstPriceData$({
              ctx: options.ctx,
              emitError,
              getParams: (item) => ({ priceFeedId: item.row.price_feed_2_id }),
              formatOutput: (item, firstPrice) => ({ ...item, firstPrice }),
            }),

            /**
             * Now we have some heuristics to fix missing prices
             *
             * 1) Missing price vaults
             * Sometimes the vault is too old for the price data to have been recorded
             * - if there is no prices
             * - AND the import state is successfully imported between the contract creation and the trx plus some safety margin
             * - THEN we set the price cache to zero
             *
             * 2) Strategist test trxs
             * Strategist testing their vaults often happen before any price is recorded
             * - if the trx price is before the first recorded price
             * - AND the import state is successful between contract creation and the first recorded price
             * - THEN we can safely set the trx price to be the first recorded price
             *
             * 3) Missing price
             * Sometimes the data source is missing prices, we want to try to interpolate
             * - if the trx has prices before and after
             * - AND the import state is successful between the closest price before and the closest price after
             * - THEN we can use the interpolated price as for this trx
             */
            Rx.connect((items$) =>
              Rx.merge(
                items$.pipe(
                  Rx.filter(({ firstPrice }) => firstPrice === null),
                  Rx.filter(({ row, successRanges, contractCreation }) => {
                    const shouldInclude = { from: contractCreation, to: new Date(row.datetime.getTime() + samplingPeriodMs["1month"]) };
                    const hasDefinitelyNoPrices = successRanges.some((range) => rangeInclude(range, shouldInclude));
                    if (!hasDefinitelyNoPrices) {
                      logger.warn({
                        msg: "Found a cache entry without a first price but import state isn't green",
                        data: { row, successRanges, contractCreation },
                      });
                    }
                    return hasDefinitelyNoPrices;
                  }),
                  // we can set the price to zero
                  Rx.map(({ row }): PricedRowType => ({ ...row, price: new Decimal(0) })),
                ),

                // we have a first price now
                items$.pipe(
                  excludeNullFields$("firstPrice"),
                  Rx.map((item) => ({ ...item, isBeforeFirstPrice: item.row.datetime < item.firstPrice.datetime })),

                  Rx.connect((items$) =>
                    Rx.merge(
                      // the trx is before the first price
                      items$.pipe(
                        Rx.filter((item) => item.isBeforeFirstPrice),
                        Rx.filter(({ row, firstPrice, successRanges, contractCreation }) => {
                          const shouldInclude = { from: contractCreation, to: firstPrice.datetime };
                          const isDefinitelyBeforeFirstPrice = successRanges.some((range) => rangeInclude(range, shouldInclude));
                          if (!isDefinitelyBeforeFirstPrice) {
                            logger.warn({
                              msg: "Found a cache entry with a trx before first price but import state isn't green",
                              data: { row, successRanges, contractCreation },
                            });
                          }
                          return isDefinitelyBeforeFirstPrice;
                        }),
                        // then the price is the first price
                        Rx.map((item): PricedRowType => ({ ...item.row, price: item.firstPrice.price })),
                      ),

                      // the trx is NOT before the first price
                      items$.pipe(
                        Rx.filter((item) => !item.isBeforeFirstPrice),
                        // try to interpolate the price
                        interpolatePrice$({
                          ctx: options.ctx,
                          bucketSize: "15min",
                          windowSize: "3days",
                          emitError,
                          getQueryParams: (item) => ({
                            datetime: item.row.datetime,
                            priceFeedId: item.row.price_feed_2_id,
                          }),
                          formatOutput: (item, interpolatedPrice) => ({ ...item, interpolatedPrice }),
                        }),
                        Rx.tap(({ row, interpolatedPrice }) => {
                          if (!interpolatedPrice) {
                            logger.warn({ msg: "Unable to interpolate price", data: { row } });
                          }
                        }),
                        excludeNullFields$("interpolatedPrice"),

                        Rx.filter(({ row, successRanges }) => {
                          const shouldInclude = {
                            from: new Date(row.datetime.getTime() - samplingPeriodMs["1day"]),
                            to: new Date(row.datetime.getTime() + samplingPeriodMs["1day"]),
                          };
                          const isDefinitelyInterpolated = successRanges.some((range) => rangeInclude(range, shouldInclude));

                          if (!isDefinitelyInterpolated) {
                            logger.warn({ msg: "Found a price to interpolate but import state isn't green", data: { row, successRanges } });
                          }
                          return isDefinitelyInterpolated;
                        }),
                        Rx.map((item): PricedRowType => ({ ...item.row, price: item.interpolatedPrice.price })),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
      // update the prices
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
            [
              objAndData.map(({ data }) => [
                data.investor_id,
                data.product_id,
                data.datetime.toISOString(),
                data.block_number,
                data.price.toString(),
              ]),
            ],
            options.ctx.client,
          );
          return new Map(objAndData.map(({ data }) => [data, data]));
        },
      }),
    ),
  );
}
