import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { samplingPeriodMs } from "../../../../types/sampling";
import { MS_PER_BLOCK_ESTIMATE } from "../../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchContractCreationInfos$ } from "../../../common/connector/contract-creation";
import { ERC20Transfer } from "../../../common/connector/erc20-transfers";
import { latestBlockNumber$ } from "../../../common/connector/latest-block-number";
import { upsertBlock$ } from "../../../common/loader/blocks";
import { createShouldIgnoreFn } from "../../../common/loader/ignore-address";
import { DbProductInvestmentImportState, addMissingImportState$ } from "../../../common/loader/import-state";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyProduct } from "../../../common/loader/product";
import { ErrorEmitter, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeResult } from "../../../common/types/import-query";
import { isProductDashboardEOL } from "../../../common/utils/eol";
import { executeSubPipeline$ } from "../../../common/utils/execute-sub-pipeline";
import { createImportStateUpdaterRunner } from "../../../common/utils/import-state-updater-runner";
import { extractObjsAndRangeFromOptimizerOutput, optimizeRangeQueries } from "../../../common/utils/optimize-range-queries";
import { ChainRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { extractProductTransfersFromOutputAndTransfers, fetchProductEvents$ } from "../../connector/product-events";
import { fetchBeefyTransferData$ } from "../../connector/transfer-data";
import { getProductContractAddress } from "../../utils/contract-accessors";
import { getInvestmentsImportStateKey } from "../../utils/import-state";
import { isBeefyBoost, isBeefyGovVault, isBeefyStandardVault } from "../../utils/type-guard";
import { upsertInvestorCacheChainInfos$ } from "./investor-cache";

const logger = rootLogger.child({ module: "beefy", component: "investment-import" });

export function createBeefyInvestmentImportRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbBeefyProduct> }) {
  return createImportStateUpdaterRunner<DbBeefyProduct, number>({
    cacheKey: "beefy:product:investment:" + options.runnerConfig.behaviour.mode,
    logInfos: { msg: "Importing historical beefy investments", data: { chain: options.chain } },
    runnerConfig: options.runnerConfig,
    getImportStateKey: getInvestmentsImportStateKey,
    pipeline$: (ctx, emitError, getLastImportedBlockNumber) => {
      const shouldIgnoreFnPromise = createShouldIgnoreFn({ client: ctx.client, chain: ctx.chain });
      return Rx.pipe(
        // create the import state if it does not exists
        addMissingImportState$<
          DbBeefyProduct,
          { product: DbBeefyProduct; importState: DbProductInvestmentImportState },
          DbProductInvestmentImportState
        >({
          ctx,
          getImportStateKey: getInvestmentsImportStateKey,
          formatOutput: (product, importState) => ({ product, importState }),
          createDefaultImportState$: Rx.pipe(
            // initialize the import state
            // find the contract creation block
            fetchContractCreationInfos$({
              ctx,
              getCallParams: (obj) => ({
                chain: ctx.chain,
                contractAddress: getProductContractAddress(obj),
              }),
              formatOutput: (obj, contractCreationInfo) => ({ obj, contractCreationInfo }),
            }),

            // drop those without a creation info
            excludeNullFields$("contractCreationInfo"),

            // add this block to our global block list
            upsertBlock$({
              ctx,
              emitError: (item, report) => {
                logger.error(mergeLogsInfos({ msg: "Failed to upsert block", data: { item } }, report.infos));
                logger.error(report.error);
                throw new Error("Failed to upsert block");
              },
              getBlockData: (item) => ({
                datetime: item.contractCreationInfo.datetime,
                chain: item.obj.chain,
                blockNumber: item.contractCreationInfo.blockNumber,
                blockData: {},
              }),
              formatOutput: (item, block) => ({ ...item, block }),
            }),

            Rx.map((item) => ({
              obj: item.obj,
              importData: {
                type: "product:investment",
                productId: item.obj.productId,
                chain: item.obj.chain,
                chainLatestBlockNumber: item.contractCreationInfo.blockNumber,
                contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
                contractCreationDate: item.contractCreationInfo.datetime,
                ranges: {
                  lastImportDate: new Date(),
                  coveredRanges: [],
                  toRetry: [],
                },
              },
            })),
          ),
        }),

        // generate our queries
        Rx.pipe(
          Rx.toArray(),
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

          Rx.map(({ items, latestBlockNumber }) =>
            optimizeRangeQueries({
              objKey: (item) => item.product.productKey,
              states: items.map(({ product, importState }) => {
                // compute recent full range in case we need it
                // fetch the last hour of data
                const maxBlocksPerQuery = ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan;
                const period = samplingPeriodMs["1hour"];
                const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[ctx.chain]);

                const lastImportedBlockNumber = getLastImportedBlockNumber();
                const diffBetweenLastImported = lastImportedBlockNumber ? latestBlockNumber - (lastImportedBlockNumber + 1) : Infinity;

                const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
                const fromBlock = latestBlockNumber - blockCountToFetch;
                const toBlock = latestBlockNumber;

                const recentFullRange = {
                  from: fromBlock - ctx.behaviour.waitForBlockPropagation,
                  to: toBlock - ctx.behaviour.waitForBlockPropagation,
                };

                let fullRange: Range<number>;

                if (ctx.behaviour.mode !== "recent" && importState !== null) {
                  // exclude latest block query from the range
                  const isLive = !isProductDashboardEOL(product);
                  const skipRecent = ctx.behaviour.skipRecentWindowWhenHistorical;
                  let doSkip = false;
                  if (skipRecent === "all") {
                    doSkip = true;
                  } else if (skipRecent === "none") {
                    doSkip = false;
                  } else if (skipRecent === "live") {
                    doSkip = isLive;
                  } else if (skipRecent === "eol") {
                    doSkip = !isLive;
                  } else {
                    throw new ProgrammerError({ msg: "Invalid skipRecentWindowWhenHistorical value", data: { skipRecent } });
                  }
                  // this is the whole range we have to cover
                  fullRange = {
                    from: importState.importData.contractCreatedAtBlock,
                    to: Math.min(latestBlockNumber - ctx.behaviour.waitForBlockPropagation, doSkip ? recentFullRange.to : Infinity),
                  };
                } else {
                  fullRange = recentFullRange;
                }

                // this can happen when we force the block number in the past and we are treating a recent product
                if (fullRange.from > fullRange.to) {
                  const importStateKey = importState?.importKey || getInvestmentsImportStateKey(product);
                  if (ctx.behaviour.forceCurrentBlockNumber !== null) {
                    logger.info({
                      msg: "current block number set too far in the past to treat this product",
                      data: { fullRange, importStateKey },
                    });
                  } else {
                    logger.error({
                      msg: "Full range is invalid",
                      data: { fullRange, importStateKey },
                    });
                  }
                }
                return {
                  obj: { product, latestBlockNumber },
                  fullRange,
                  coveredRanges: importState?.importData.ranges.coveredRanges || [],
                  toRetry: importState?.importData.ranges.toRetry || [],
                };
              }),
              options: {
                ignoreImportState: ctx.behaviour.ignoreImportState,
                maxAddressesPerQuery: ctx.rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize || 1,
                maxQueriesPerProduct: ctx.behaviour.limitQueriesCountTo.investment,
                maxRangeSize: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
              },
            }),
          ),
          Rx.concatAll(),
        ),

        // detect interesting events in this ranges
        fetchProductEvents$({
          ctx,
          emitError: (query, report) =>
            extractObjsAndRangeFromOptimizerOutput(query).map(({ obj, range }) =>
              emitError({ target: obj.product, latest: obj.latestBlockNumber, range }, report),
            ),
          getCallParams: (query) => query,
          formatOutput: (query, transfers) =>
            extractProductTransfersFromOutputAndTransfers(query, (o) => o.product, transfers).flatMap(
              ({ obj: { latestBlockNumber, product }, range, transfers }) => ({ latestBlockNumber, product, range, transfers }),
            ),
        }),
        Rx.concatAll(),

        // then for each query, do the import
        executeSubPipeline$({
          ctx,
          emitError: ({ product, latestBlockNumber, range }, report) => emitError({ target: product, latest: latestBlockNumber, range }, report),
          getObjs: async ({ product, latestBlockNumber, range, transfers }) => {
            const shouldIgnoreFn = await shouldIgnoreFnPromise;
            return transfers
              .map((transfer): TransferToLoad => ({ range, transfer, product, latest: latestBlockNumber }))
              .filter((transfer) => {
                const shouldIgnore = shouldIgnoreFn(transfer.transfer.ownerAddress);
                if (shouldIgnore) {
                  logger.trace({ msg: "ignoring transfer", data: { chain: ctx.chain, transfer } });
                } else {
                  logger.trace({ msg: "not ignoring transfer", data: { chain: ctx.chain, ownerAddress: transfer.transfer.ownerAddress } });
                }
                return !shouldIgnore;
              });
          },
          pipeline: (emitError) => loadTransfers$({ ctx: ctx, emitError }),
          formatOutput: (item, _ /* we don't care about the result */) => item,
        }),

        Rx.map(
          ({ product, latestBlockNumber, range }): ImportRangeResult<DbBeefyProduct, number> => ({
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

type TransferToLoad<TProduct extends DbBeefyProduct = DbBeefyProduct> = {
  transfer: ERC20Transfer;
  product: TProduct;
  range: Range<number>;
  latest: number;
};

function loadTransfers$<TObj, TInput extends { parent: TObj; target: TransferToLoad<DbBeefyProduct> }, TErr extends ErrorEmitter<TInput>>(options: {
  ctx: ImportCtx;
  emitError: TErr;
}) {
  return Rx.pipe(
    Rx.tap((item: TInput) => logger.trace({ msg: "loading transfer", data: { chain: options.ctx.chain, transferData: item } })),

    fetchBeefyTransferData$({
      ctx: options.ctx,
      emitError: options.emitError,
      getCallParams: (item) => {
        const balance = {
          decimals: item.target.transfer.tokenDecimals,
          contractAddress: item.target.transfer.tokenAddress,
          ownerAddress: item.target.transfer.ownerAddress,
        };
        const blockNumber = item.target.transfer.blockNumber;
        if (isBeefyStandardVault(item.target.product)) {
          return {
            ppfs: {
              vaultAddress: item.target.product.productData.vault.contract_address,
              underlyingDecimals: item.target.product.productData.vault.want_decimals,
              vaultDecimals: item.target.product.productData.vault.token_decimals,
            },
            balance,
            blockNumber,
            fetchPpfs: true,
          };
        } else if (isBeefyBoost(item.target.product)) {
          return {
            ppfs: {
              vaultAddress: item.target.product.productData.boost.staked_token_address,
              underlyingDecimals: item.target.product.productData.boost.vault_want_decimals,
              vaultDecimals: item.target.product.productData.boost.staked_token_decimals,
            },
            balance,
            blockNumber,
            fetchPpfs: true,
          };
        } else if (isBeefyGovVault(item.target.product)) {
          return {
            balance,
            blockNumber,
            fetchPpfs: false,
          };
        }
        logger.error({ msg: "Unsupported product type", data: { product: item.target.product } });
        throw new Error("Unsupported product type");
      },
      formatOutput: (item, { balance, blockDatetime, shareRate }) => ({ ...item, blockDatetime, balance, shareRate }),
    }),

    // ==============================
    // now we are ready for the insertion
    // ==============================

    // insert the block data
    upsertBlock$({
      ctx: options.ctx,
      emitError: options.emitError,
      getBlockData: (item) => ({
        blockNumber: item.target.transfer.blockNumber,
        chain: options.ctx.chain,
        datetime: item.blockDatetime,
      }),
      formatOutput: (item, investorId) => ({ ...item, investorId }),
    }),

    // insert the investor data
    upsertInvestor$({
      ctx: options.ctx,
      emitError: options.emitError,
      getInvestorData: (item) => ({
        address: item.target.transfer.ownerAddress,
        investorData: {},
      }),
      formatOutput: (item, investorId) => ({ ...item, investorId }),
    }),

    // insert ppfs as a price
    upsertPrice$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPriceData: (item) => ({
        priceFeedId: item.target.product.priceFeedId1,
        blockNumber: item.target.transfer.blockNumber,
        price: item.shareRate,
        datetime: item.blockDatetime,
      }),
      formatOutput: (item, priceRow) => ({ ...item, priceRow }),
    }),

    // insert the investment data
    upsertInvestment$({
      ctx: options.ctx,
      emitError: options.emitError,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.target.transfer.blockNumber,
        productId: item.target.product.productId,
        investorId: item.investorId,
        transactionHash: item.target.transfer.transactionHash,
        // balance is expressed in vault shares
        balance: item.balance,
        balanceDiff: item.target.transfer.amountTransferred,
        pendingRewards: null,
        pendingRewardsDiff: null,
      }),
      formatOutput: (item, investment) => ({ ...item, investment, result: true }),
    }),

    // push all this data to the investor cache so we can use it later
    upsertInvestorCacheChainInfos$({
      ctx: options.ctx,
      emitError: options.emitError,
      getInvestorCacheChainInfos: (item) => ({
        product: item.target.product,
        data: {
          productId: item.investment.productId,
          investorId: item.investment.investorId,
          datetime: item.investment.datetime,
          blockNumber: item.investment.blockNumber,
          transactionHash: item.target.transfer.transactionHash,
          balance: item.investment.balance,
          balanceDiff: item.investment.balanceDiff,
          pendingRewards: null,
          pendingRewardsDiff: null,
          shareToUnderlyingPrice: item.shareRate,
          underlyingBalance: item.investment.balance.mul(item.shareRate),
          underlyingDiff: item.investment.balanceDiff.mul(item.shareRate),
        },
      }),
      formatOutput: (item, investorCacheChainInfos) => ({ ...item, investorCacheChainInfos }),
    }),
  );
}
