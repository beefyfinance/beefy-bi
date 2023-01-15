import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { addRegularIntervalBlockRangesQueries } from "../../../common/connector/import-queries";
import { fetchPriceFeedContractCreationInfos } from "../../../common/loader/fetch-product-creation-infos";
import { DbProductInvestmentImportState, DbProductShareRateImportState, fetchImportState$ } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { fetchProduct$ } from "../../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery, ImportRangeResult } from "../../../common/types/import-query";
import { createHistoricalImportRunner } from "../../../common/utils/historical-recent-pipeline";
import { ChainRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { isBeefyBoost, isBeefyGovVault } from "../../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "share-rate-import" });

export function createBeefyHistoricalShareRatePricesRunner(options: {
  chain: Chain;
  forceCurrentBlockNumber: number | null;
  runnerConfig: ChainRunnerConfig<DbPriceFeed>;
}) {
  return createHistoricalImportRunner<DbPriceFeed, number, DbProductShareRateImportState>({
    runnerConfig: options.runnerConfig,
    logInfos: { msg: "Importing historical share rate prices", data: { chain: options.chain } },
    getImportStateKey: (priceFeed) => `price:feed:${priceFeed.priceFeedId}`,
    isLiveItem: (target) => target.priceFeedData.active,
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
        Rx.map((obj) => ({ obj })),
        // find the first date we are interested in
        // so we need the first creation date of each product
        fetchPriceFeedContractCreationInfos({
          ctx,
          emitError: (item, report) => {
            logger.error(mergeLogsInfos({ msg: "Error while fetching price feed contract creation infos. ", data: item }, report.infos));
            logger.error(report.error);
            throw new Error("Error while fetching price feed creation infos. " + item.obj.priceFeedId);
          },
          importStateType: "product:investment", // we want to find the contract creation date we already fetched from the investment pipeline
          which: "price-feed-1", // we work on the first applied price
          productType: "beefy:vault",
          getPriceFeedId: (item) => item.obj.priceFeedId,
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        Rx.map((item) => ({
          obj: item.obj,
          importData: {
            type: "product:share-rate",
            priceFeedId: item.obj.priceFeedId,
            chain: item.contractCreationInfo.chain,
            productId: item.contractCreationInfo.productId,
            chainLatestBlockNumber: 0,
            contractCreatedAtBlock: item.contractCreationInfo.contractCreatedAtBlock,
            contractCreationDate: item.contractCreationInfo.contractCreationDate,
            ranges: {
              lastImportDate: new Date(),
              coveredRanges: [],
              toRetry: [],
            },
          },
        })),
      ),
    generateQueries$: (ctx) =>
      Rx.pipe(
        // fetch the parent import state
        fetchImportState$({
          client: ctx.client,
          streamConfig: ctx.streamConfig,
          getImportStateKey: (item) => `product:investment:${item.importState.importData.productId}`,
          formatOutput: (item, parentImportState: DbProductInvestmentImportState | null) => ({ ...item, parentImportState }),
        }),
        excludeNullFields$("parentImportState"),

        addRegularIntervalBlockRangesQueries({
          ctx,
          emitError: (item, report) => {
            logger.error(mergeLogsInfos({ msg: "Error while adding covering block ranges", data: item }, report.infos));
            logger.error(report.error);
            throw new Error("Error while adding covering block ranges");
          },
          chain: options.chain,
          timeStep: "15min",
          forceCurrentBlockNumber: options.forceCurrentBlockNumber,
          getImportState: (item) => item.importState,
          formatOutput: (item, latestBlockNumber, blockRanges) => blockRanges.map((range) => ({ ...item, range, latest: latestBlockNumber })),
        }),
        Rx.concatAll(),
      ),
    processImportQuery$: (ctx, emitError) => processShareRateQuery$({ ctx, emitError }),
  });
}

function processShareRateQuery$<
  TObj extends ImportRangeQuery<DbPriceFeed, number> & { importState: DbProductShareRateImportState },
  TErr extends ErrorEmitter<TObj>,
>(options: { ctx: ImportCtx; emitError: TErr }): Rx.OperatorFunction<TObj, ImportRangeResult<DbPriceFeed, number>> {
  return Rx.pipe(
    // get the midpoint of the range
    Rx.map((item) => ({ ...item, rangeMidpoint: Math.floor((item.range.from + item.range.to) / 2) })),

    fetchProduct$({
      ctx: options.ctx,
      emitError: options.emitError,
      getProductId: (item) => item.importState.importData.productId,
      formatOutput: (item, product) => ({ ...item, product }),
    }),

    fetchBeefyPPFS$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPPFSCallParams: (item) => {
        if (isBeefyBoost(item.product)) {
          throw new ProgrammerError("beefy boost do not have ppfs");
        }
        if (isBeefyGovVault(item.product)) {
          throw new ProgrammerError("beefy gov vaults do not have ppfs");
        }
        const vault = item.product.productData.vault;
        return {
          underlyingDecimals: vault.want_decimals,
          vaultAddress: vault.contract_address,
          vaultDecimals: vault.token_decimals,
          blockNumber: item.rangeMidpoint,
        };
      },
      formatOutput: (item, ppfs) => ({ ...item, ppfs }),
    }),

    // add block datetime
    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: options.emitError,
      getBlockNumber: (item) => item.rangeMidpoint,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    upsertPrice$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPriceData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.rangeMidpoint,
        priceFeedId: item.target.priceFeedId,
        price: item.ppfs,
        priceData: { from: "ppfs-snapshots", query: { range: item.range, midPoint: item.rangeMidpoint, latest: item.latest } },
      }),
      formatOutput: (priceData, price) => ({ ...priceData, price }),
    }),

    // transform to result
    Rx.map((item) => ({ ...item, success: true })),
  );
}
