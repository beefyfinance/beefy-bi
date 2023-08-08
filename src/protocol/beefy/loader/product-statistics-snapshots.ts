import { min } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { SamplingPeriod } from "../../../types/sampling";
import { MS_PER_BLOCK_ESTIMATE } from "../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { isValidRange } from "../../../utils/range";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { latestBlockNumber$ } from "../../common/connector/latest-block-number";
import { upsertBeefyVaultStats$ } from "../../common/loader/beefy-vault-stats";
import { fetchChainBlockList$ } from "../../common/loader/chain-block-list";
import { fetchProductCreationInfos$ } from "../../common/loader/fetch-product-creation-infos";
import { DbProductStatisticsImportState, addMissingImportState$, fetchImportState$ } from "../../common/loader/import-state";
import { DbBeefyStdVaultProduct, DbProduct } from "../../common/loader/product";
import { ImportRangeResult } from "../../common/types/import-query";
import { isProductDashboardEOL } from "../../common/utils/eol";
import { createImportStateUpdaterRunner } from "../../common/utils/import-state-updater-runner";
import { importStateToOptimizerRangeInput } from "../../common/utils/query/import-state-to-range-input";
import { optimizeQueries } from "../../common/utils/query/optimize-queries";
import { createOptimizerIndexFromBlockList } from "../../common/utils/query/optimizer-index-from-block-list";
import { extractObjsAndRangeFromOptimizerOutput } from "../../common/utils/query/optimizer-utils";
import { ChainRunnerConfig } from "../../common/utils/rpc-chain-runner";
import { extractProductStatisticsFromOptimizerOutput, fetchMultipleProductStatistics$ } from "../connector/product-stats-single-block-snapshots";
import { getProductStatisticsImportStateKey } from "../utils/import-state";
import { isBeefyStandardVault } from "../utils/type-guard";

const logger = rootLogger.child({ module: "beefy", component: "investment-import" });

export function createBeefyProductStatisticsRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbProduct> }) {
  const SNAPSHOT_INTERVAL: SamplingPeriod = "1day";

  return createImportStateUpdaterRunner<DbProduct, number>({
    cacheKey: "beefy:product:statistics-snapshot:" + options.runnerConfig.behaviour.mode,
    logInfos: { msg: "Importing historical beefy product stats snapshots", data: { chain: options.chain } },
    runnerConfig: options.runnerConfig,
    getImportStateKey: getProductStatisticsImportStateKey,
    pipeline$: (ctx, emitError, getLastImportedBlockNumber) => {
      const createImportStateIfNeeded$: Rx.OperatorFunction<DbProduct, { product: DbProduct; importState: DbProductStatisticsImportState | null }> =
        ctx.behaviour.mode === "recent"
          ? Rx.pipe(
              fetchImportState$({
                client: ctx.client,
                getImportStateKey: (product) => getProductStatisticsImportStateKey({ productId: product.productId }),
                streamConfig: ctx.streamConfig,
                formatOutput: (product, importState) => ({ product, importState: importState as DbProductStatisticsImportState | null }),
              }),
            )
          : Rx.pipe(
              // only consider beefy standard vaults
              Rx.filter((item): item is DbBeefyStdVaultProduct => isBeefyStandardVault(item)),
              addMissingImportState$<
                DbBeefyStdVaultProduct,
                { product: DbProduct; importState: DbProductStatisticsImportState },
                DbProductStatisticsImportState
              >({
                ctx,
                getImportStateKey: getProductStatisticsImportStateKey,
                formatOutput: (product, importState) => ({ product, importState }),
                createDefaultImportState$: Rx.pipe(
                  // find the first date we are interested in this price
                  fetchProductCreationInfos$({
                    ctx,
                    emitError: (item, report) => {
                      logger.error(mergeLogsInfos({ msg: "Failed to fetch product creation info", data: { item } }, report.infos));
                      logger.error(report.error);
                      throw new Error("Error while fetching product creation infos for product" + item.productId);
                    },
                    getProductId: (item) => item.productId,
                    formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
                  }),

                  // drop those without a creation info
                  excludeNullFields$("contractCreationInfo"),

                  Rx.map((item) => ({
                    obj: item,
                    importData: {
                      type: "product:statistics",
                      productId: item.productId,
                      chain: ctx.chain,
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
              }),
            );

      return Rx.pipe(
        Rx.pipe(
          // create the import state if it does not exists
          createImportStateIfNeeded$,

          excludeNullFields$("importState"),
        ),

        // only consider beefy standard vaults
        Rx.filter((item): item is { product: DbBeefyStdVaultProduct; importState: DbProductStatisticsImportState } =>
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
            getFirstBlock: (item) => min(item.items.map((i) => i.importState.importData.contractCreatedAtBlock)) as number,
            latestBlock: (item) => ({ approximativeBlockDatetime: new Date(), blockNumber: item.latestBlockNumber }),
            formatOutput: (item, blockList) => ({ ...item, blockList }),
          }),

          Rx.map(({ items, latestBlockNumber, blockList }) =>
            optimizeQueries(
              {
                objKey: (item) => item.product.productId + "",
                states: items
                  .map(({ product, importState }) => {
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
                    return { obj: { product, latestBlockNumber }, ...filteredImportState };
                  })
                  // this can happen if we restrict a very recent product with forceConsideredBlockRange
                  .filter((state) => isValidRange(state.fullRange)),

                options: {
                  ignoreImportState: ctx.behaviour.ignoreImportState,
                  maxAddressesPerQuery: ctx.rpcConfig.rpcLimitations.methods.eth_call || 1,
                  maxQueriesPerProduct: ctx.behaviour.limitQueriesCountTo.shareRate,
                  maxRangeSize: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
                },
              },
              () =>
                createOptimizerIndexFromBlockList({
                  blockNumberList: blockList.map((b) => b.interpolated_block_number),
                  latestBlockNumber: latestBlockNumber - ctx.behaviour.waitForBlockPropagation,
                  firstBlockToConsider: Math.min(
                    min(items.map((item) => item.importState.importData.contractCreatedAtBlock)) as number,
                    blockList.length > 0 ? blockList[0].interpolated_block_number : latestBlockNumber,
                  ),
                  snapshotInterval: SNAPSHOT_INTERVAL,
                  msPerBlockEstimate: MS_PER_BLOCK_ESTIMATE[options.chain],
                  maxBlocksPerQuery: Infinity,
                }),
            ),
          ),
          Rx.concatAll(),
        ),

        // get the share rate data
        fetchMultipleProductStatistics$({
          ctx,
          emitError: (query, report) =>
            extractObjsAndRangeFromOptimizerOutput({ output: query, objKey: (o) => o.product.productId + "" }).map(({ obj, range }) =>
              emitError({ target: obj.product, latest: obj.latestBlockNumber, range }, report),
            ),
          getCallParams: (query) => query,
          formatOutput: (query, productStatisticsResults) =>
            extractProductStatisticsFromOptimizerOutput(query, (o) => o.product, productStatisticsResults).flatMap(({ obj, range, result }) => ({
              ...obj,
              range,
              ...result,
            })),
        }),
        Rx.concatAll(),

        upsertBeefyVaultStats$({
          ctx: ctx,
          emitError: ({ product, latestBlockNumber, range }, report) => emitError({ target: product, latest: latestBlockNumber, range }, report),
          getVaultStatsData: (item) => ({
            productId: item.product.productId,
            datetime: item.blockDatetime,
            blockNumber: item.blockNumber,
            shareToUnderlyingPrice: item.vaultShareRate,
            vaultTotalSupply: item.vaultTotalSupply,
            stakedUnderlying: item.stakedUnderlying,
            underlyingTotalSupply: item.underlyingTotalSupply,
            underlyingCapturePercentage: item.stakedUnderlying.dividedBy(item.underlyingTotalSupply).toNumber(),
          }),
          formatOutput: (item, _) => item,
        }),

        Rx.map(
          ({ product, latestBlockNumber, range }): ImportRangeResult<DbProduct, number> => ({
            success: true,
            latest: latestBlockNumber,
            range,
            target: product,
          }),
        ),
      );
    },
  });
}
