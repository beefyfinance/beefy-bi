import { min } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { SamplingPeriod } from "../../../../types/sampling";
import { MS_PER_BLOCK_ESTIMATE } from "../../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { isValidRange } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { latestBlockNumber$ } from "../../../common/connector/latest-block-number";
import { fetchChainBlockList$ } from "../../../common/loader/chain-block-list";
import { fetchPriceFeedContractCreationInfos } from "../../../common/loader/fetch-product-creation-infos";
import { DbProductShareRateImportState, addMissingImportState$ } from "../../../common/loader/import-state";
import { DbPriceFeed } from "../../../common/loader/price-feed";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyStdVaultProduct, fetchProduct$ } from "../../../common/loader/product";
import { ImportRangeResult } from "../../../common/types/import-query";
import { isProductDashboardEOL } from "../../../common/utils/eol";
import { createImportStateUpdaterRunner } from "../../../common/utils/import-state-updater-runner";
import { importStateToOptimizerRangeInput } from "../../../common/utils/query/import-state-to-range-input";
import { optimizeQueries } from "../../../common/utils/query/optimize-queries";
import { createOptimizerIndexFromBlockList } from "../../../common/utils/query/optimizer-index-from-block-list";
import { extractObjsAndRangeFromOptimizerOutput } from "../../../common/utils/query/optimizer-utils";
import { ChainRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { extractShareRateFromOptimizerOutput, fetchMultipleShareRate$ } from "../../connector/share-rate/share-rate-single-block-snapshots";
import { getPriceFeedImportStateKey } from "../../utils/import-state";
import { isBeefyStandardVault } from "../../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "investment-import" });

export function createBeefyShareRateSnapshotsRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbPriceFeed> }) {
  const SNAPSHOT_INTERVAL: SamplingPeriod = "15min";

  return createImportStateUpdaterRunner<DbPriceFeed, number>({
    cacheKey: "beefy:product:share-rate:" + options.runnerConfig.behaviour.mode,
    logInfos: { msg: "Importing historical beefy investments", data: { chain: options.chain } },
    runnerConfig: options.runnerConfig,
    getImportStateKey: getPriceFeedImportStateKey,
    pipeline$: (ctx, emitError, getLastImportedBlockNumber) => {
      const createImportStateIfNeeded$: Rx.OperatorFunction<
        DbPriceFeed,
        { priceFeed: DbPriceFeed; importState: DbProductShareRateImportState | null }
      > =
        ctx.behaviour.mode === "recent"
          ? Rx.pipe(Rx.map((priceFeed) => ({ priceFeed, importState: null })))
          : addMissingImportState$<
              DbPriceFeed,
              { priceFeed: DbPriceFeed; importState: DbProductShareRateImportState },
              DbProductShareRateImportState
            >({
              ctx,
              getImportStateKey: getPriceFeedImportStateKey,
              formatOutput: (priceFeed, importState) => ({ priceFeed, importState }),
              createDefaultImportState$: Rx.pipe(
                fetchPriceFeedContractCreationInfos({
                  ctx,
                  emitError: (item, report) => {
                    logger.error(mergeLogsInfos({ msg: "Error while fetching price feed contract creation infos. ", data: item }, report.infos));
                    logger.error(report.error);
                    throw new Error("Error while fetching price feed creation infos. " + item.priceFeedId);
                  },
                  importStateType: "product:investment", // we want to find the contract creation date we already fetched from the investment pipeline
                  which: "price-feed-1", // we work on the first applied price
                  productType: "beefy:vault",
                  getPriceFeedId: (item) => item.priceFeedId,
                  formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
                }),

                // drop those without a creation info
                excludeNullFields$("contractCreationInfo"),

                Rx.map((item) => ({
                  obj: item,
                  importData: {
                    type: "product:share-rate",
                    priceFeedId: item.priceFeedId,
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
            });

      return Rx.pipe(
        // create the import state if it does not exists
        createImportStateIfNeeded$,

        excludeNullFields$("importState"),

        // get the associated product
        fetchProduct$({
          ctx,
          emitError: (items, report) => {
            logger.error(mergeLogsInfos({ msg: "Failed to get latest block number block", data: { items } }, report.infos));
            logger.error(report.error);
            throw new Error("Failed to get latest block number block");
          },
          getProductId: (item) => item.importState.importData.productId,
          formatOutput: (item, product) => ({ ...item, product }),
        }),

        Rx.filter((item): item is { priceFeed: DbPriceFeed; product: DbBeefyStdVaultProduct; importState: DbProductShareRateImportState } =>
          isBeefyStandardVault(item.product),
        ),

        // generate our queries
        Rx.pipe(
          // like Rx.toArray() but non blocking if import state creation takes too much time
          Rx.bufferTime(ctx.streamConfig.maxInputWaitMs),
          Rx.filter((objs) => objs.length > 0),

          // go get the latest block number for this chain
          latestBlockNumber$({
            ctx: ctx,
            emitError: (items, report) => {
              logger.error(mergeLogsInfos({ msg: "Failed to get latest block number block", data: { items } }, report.infos));
              logger.error(report.error);
              throw new Error("Failed to get latest block number block");
            },
            formatOutput: (items, latestBlockNumber) => ({ items, latestBlockNumber }),
          }),

          // find out an interpolation of block numbers at SNAPSHOT_INTERVAL intervals
          fetchChainBlockList$({
            ctx: ctx,
            emitError: (item, report) => {
              logger.error(mergeLogsInfos({ msg: "Error while fetching the chain block list", data: item }, report.infos));
              logger.error(report.error);
              throw new Error("Error while adding covering block ranges");
            },
            getChain: () => options.chain,
            timeStep: SNAPSHOT_INTERVAL,
            getFirstDate: (item) => min(item.items.map((i) => i.importState.importData.contractCreationDate)) as Date,
            formatOutput: (item, blockList) => ({ ...item, blockList }),
          }),

          Rx.map(({ items, latestBlockNumber, blockList }) =>
            optimizeQueries(
              {
                objKey: (item) => item.priceFeed.feedKey,
                states: items
                  .map(({ priceFeed, product, importState }) => {
                    const lastImportedBlockNumber = getLastImportedBlockNumber();
                    const isLive = !isProductDashboardEOL(product);
                    const filteredImportState = importStateToOptimizerRangeInput({
                      importState,
                      latestBlockNumber,
                      behaviour: ctx.behaviour,
                      isLive,
                      lastImportedBlockNumber,
                      maxBlocksPerQuery: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
                      msPerBlockEstimate: MS_PER_BLOCK_ESTIMATE[ctx.chain],
                    });
                    return { obj: { priceFeed, product, latestBlockNumber }, ...filteredImportState };
                  })
                  // this can happen if we restrict a very recent product with forceConsideredBlockRange
                  .filter((state) => isValidRange(state.fullRange)),

                options: {
                  ignoreImportState: ctx.behaviour.ignoreImportState,
                  maxAddressesPerQuery: ctx.rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize || 1,
                  maxQueriesPerProduct: ctx.behaviour.limitQueriesCountTo.investment,
                  maxRangeSize: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
                },
              },
              () =>
                createOptimizerIndexFromBlockList({
                  mode: ctx.behaviour.mode,
                  blockNumberList: blockList.map((b) => b.interpolated_block_number),
                  latestBlockNumber: latestBlockNumber - ctx.behaviour.waitForBlockPropagation,
                  firstBlockToConsider: Math.min(
                    min(items.map((item) => item.importState.importData.contractCreatedAtBlock)) as number,
                    blockList.length > 0 ? blockList[0].interpolated_block_number : latestBlockNumber,
                  ),
                  snapshotInterval: SNAPSHOT_INTERVAL,
                  msPerBlockEstimate: MS_PER_BLOCK_ESTIMATE[options.chain],
                  maxBlocksPerQuery: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
                }),
            ),
          ),
          Rx.concatAll(),
        ),

        // get the share rate data
        fetchMultipleShareRate$({
          ctx,
          emitError: (query, report) =>
            extractObjsAndRangeFromOptimizerOutput({ output: query, objKey: (o) => o.priceFeed.feedKey }).map(({ obj, range }) =>
              emitError({ target: obj.priceFeed, latest: obj.latestBlockNumber, range }, report),
            ),
          getCallParams: (query) => query,
          formatOutput: (query, shareRateResults) =>
            extractShareRateFromOptimizerOutput(query, (o) => o.product, shareRateResults).flatMap(({ obj, range, result }) => ({
              ...obj,
              range,
              ...result,
            })),
        }),
        Rx.concatAll(),

        upsertPrice$({
          ctx: ctx,
          emitError: ({ priceFeed, latestBlockNumber, range }, report) => emitError({ target: priceFeed, latest: latestBlockNumber, range }, report),
          getPriceData: (item) => ({
            datetime: item.blockDatetime,
            blockNumber: item.blockNumber,
            priceFeedId: item.priceFeed.priceFeedId,
            price: item.shareRate,
            priceData: { from: "p" /** ppfs-snapshots */, query: { range: item.range, midPoint: item.blockNumber, latest: item.latestBlockNumber } },
          }),
          formatOutput: (priceData, price) => ({ ...priceData, price }),
        }),

        Rx.map(
          ({ priceFeed, latestBlockNumber, range }): ImportRangeResult<DbPriceFeed, number> => ({
            success: true,
            latest: latestBlockNumber,
            range,
            target: priceFeed,
          }),
        ),
      );
    },
  });
}
