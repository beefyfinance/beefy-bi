import Decimal from "decimal.js";
import * as Rx from "rxjs";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { addHistoricalDateQuery$, addLatestDateQuery$ } from "../../../common/connector/import-queries";
import { fetchProductCreationInfos$ } from "../../../common/loader/fetch-product-creation-infos";
import { DbOraclePriceImportState } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { refreshPriceCachesIfRequested$, upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyProduct } from "../../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../../../common/types/import-query";
import { executeSubPipeline$ } from "../../../common/utils/execute-sub-pipeline";
import { createHistoricalImportRunner, createRecentImportRunner } from "../../../common/utils/historical-recent-pipeline";
import { NoRpcRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { fetchBeefyDataPrices$ } from "../../connector/prices";
import { getPriceFeedImportStateKey } from "../../utils/import-state";

type UnderlyingPriceFeedInput = {
  product: DbBeefyProduct;
  priceFeed: DbPriceFeed;
};

const logger = rootLogger.child({ module: "beefy", component: "import-underlying-prices" });

export function createBeefyHistoricalUnderlyingPricesRunner(options: { runnerConfig: NoRpcRunnerConfig<UnderlyingPriceFeedInput> }) {
  return createHistoricalImportRunner<UnderlyingPriceFeedInput, Date, DbOraclePriceImportState>({
    runnerConfig: options.runnerConfig,
    logInfos: { msg: "Importing historical underlying prices" },
    getImportStateKey: (item) => getPriceFeedImportStateKey(item.priceFeed),
    isLiveItem: (target) => target.priceFeed.priceFeedData.active,
    generateQueries$: (ctx) =>
      Rx.pipe(
        addHistoricalDateQuery$({
          ctx,
          getImport: (item) => item.importState,
          getFirstDate: (importState) => importState.importData.firstDate,
          formatOutput: (item, latestDate, queries) => queries.map((range) => ({ ...item, latest: latestDate, range })),
        }),
        Rx.concatAll(),
      ),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        Rx.map((obj) => ({ obj })),
        // initialize the import state

        // find the first date we are interested in this price
        fetchProductCreationInfos$({
          ctx,
          emitError: (item, report) => {
            logger.error(mergeLogsInfos({ msg: "Failed to fetch product creation info", data: { item } }, report.infos));
            logger.error(report.error);
            throw new Error("Error while fetching product creation infos for price feed" + item.obj.priceFeed.priceFeedId);
          },
          getProductId: (item) => item.obj.product.productId,
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        Rx.map((item) => ({
          obj: item.obj,
          importData: {
            type: "oracle:price",
            priceFeedId: item.obj.priceFeed.priceFeedId,
            firstDate: item.contractCreationInfo.contractCreationDate,
            ranges: {
              lastImportDate: new Date(),
              coveredRanges: [],
              toRetry: [],
            },
          },
        })),
      ),
    processImportQuery$: (ctx, emitError) => insertPricePipeline$({ ctx, emitError }),
  });
}

export function createBeefyRecentUnderlyingPricesRunner(options: { runnerConfig: NoRpcRunnerConfig<UnderlyingPriceFeedInput> }) {
  return createRecentImportRunner<UnderlyingPriceFeedInput, Date, DbOraclePriceImportState>({
    runnerConfig: options.runnerConfig,
    cacheKey: "beefy:underlying:prices:recent",
    logInfos: { msg: "Importing beefy recent underlying prices" },
    getImportStateKey: (item) => getPriceFeedImportStateKey(item.priceFeed),
    isLiveItem: (target) => target.priceFeed.priceFeedData.active,
    generateQueries$: ({ ctx, lastImported, formatOutput }) =>
      addLatestDateQuery$({
        ctx,
        getLastImportedDate: () => lastImported,
        formatOutput: (item, latestDate, query) => formatOutput(item, latestDate, [query]),
      }),
    processImportQuery$: (ctx, emitError) => insertPricePipeline$({ ctx, emitError }),
  });
}

function insertPricePipeline$<TObj extends ImportRangeQuery<UnderlyingPriceFeedInput, Date>, TErr extends ErrorEmitter<TObj>>(options: {
  ctx: ImportCtx;
  emitError: TErr;
}) {
  return Rx.pipe(
    Rx.tap((_: TObj) => {}),

    fetchBeefyDataPrices$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPriceParams: (item) => ({
        oracleId: item.target.priceFeed.priceFeedData.externalId,
        samplingPeriod: "15min",
        range: item.range,
        cacheBusting: options.ctx.behaviour.beefyPriceDataCacheBusting,
      }),
      formatOutput: (item, prices) => ({ ...item, prices }),
    }),

    executeSubPipeline$({
      ctx: options.ctx,
      emitError: options.emitError,
      getObjs: async (item) => item.prices,
      pipeline: (emitError) =>
        Rx.pipe(
          upsertPrice$({
            ctx: options.ctx,
            emitError,
            getPriceData: (item) => ({
              datetime: item.target.datetime,
              blockNumber: Math.floor(item.target.datetime.getTime() / 1000),
              priceFeedId: item.parent.target.priceFeed.priceFeedId,
              price: new Decimal(item.target.value),
              priceData: {},
            }),
            formatOutput: (priceData, price) => ({ ...priceData, result: price }),
          }),
        ),
      formatOutput: (item, prices) => ({ ...item, prices, success: true }),
    }),

    // refresh caches if needed
    refreshPriceCachesIfRequested$({
      ctx: options.ctx,
      emitError: (obj, report) => options.emitError(obj, report),
      getParams: (item) => ({
        priceFeedId: item.target.priceFeed.priceFeedId,
        range: item.range,
      }),
    }),

    // format output
    Rx.map(
      (item): ImportRangeResult<UnderlyingPriceFeedInput, Date> => ({
        target: item.target,
        latest: item.latest,
        range: item.range,
        success: item.success,
      }),
    ),
  );
}
