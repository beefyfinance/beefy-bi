import { groupBy } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { getBridgedVaultOriginChains, getBridgedVaultTargetChains } from "../../../../utils/addressbook";
import { MS_PER_BLOCK_ESTIMATE } from "../../../../utils/config";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, isValidRange, rangeExcludeMany } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchBlockFromDatetime$ } from "../../../common/connector/block-from-datetime";
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
import { importStateToOptimizerRangeInput } from "../../../common/utils/query/import-state-to-range-input";
import { optimizeQueries } from "../../../common/utils/query/optimize-queries";
import { createOptimizerIndexFromState } from "../../../common/utils/query/optimizer-index-from-state";
import { extractObjsAndRangeFromOptimizerOutput } from "../../../common/utils/query/optimizer-utils";
import { ChainRunnerConfig, createChainPipeline } from "../../../common/utils/rpc-chain-runner";
import { extractProductTransfersFromOutputAndTransfers, fetchProductEvents$ } from "../../connector/product-events";
import { fetchBeefyTransferData$ } from "../../connector/transfer-data";
import { getProductContractAddress } from "../../utils/contract-accessors";
import { getInvestmentsImportStateKey } from "../../utils/import-state";
import { isBeefyBoost, isBeefyBridgedVault, isBeefyGovVault, isBeefyStandardVault } from "../../utils/type-guard";
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

      const createImportStateIfNeeded$: Rx.OperatorFunction<
        DbBeefyProduct,
        { product: DbBeefyProduct; importState: DbProductInvestmentImportState | null }
      > =
        ctx.behaviour.mode === "recent"
          ? Rx.pipe(Rx.map((product) => ({ product, importState: null })))
          : addMissingImportState$<
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
            });

      return Rx.pipe(
        // create the import state if it does not exists
        createImportStateIfNeeded$,

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

          Rx.map(({ items, latestBlockNumber }) =>
            optimizeQueries(
              {
                objKey: (item) => item.product.productKey,
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
                  maxAddressesPerQuery: ctx.rpcConfig.rpcLimitations.maxGetLogsAddressBatchSize || 1,
                  maxQueriesPerProduct: ctx.behaviour.limitQueriesCountTo.investment,
                  maxRangeSize: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
                },
              },
              (rangesToQuery) =>
                createOptimizerIndexFromState(
                  rangesToQuery.map((s) => s.ranges),
                  {
                    mergeIfCloserThan: Math.round(ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan / 2),
                    verticalSlicesSize: ctx.rpcConfig.rpcLimitations.maxGetLogsBlockSpan,
                  },
                ),
            ),
          ),
          Rx.concatAll(),
        ),

        // detect interesting events in this ranges
        Rx.pipe(
          fetchProductEvents$({
            ctx,
            emitError: (query, report) =>
              extractObjsAndRangeFromOptimizerOutput({ output: query, objKey: (o) => o.product.productKey }).map(({ obj, range }) =>
                emitError({ target: obj.product, latest: obj.latestBlockNumber, range }, report),
              ),
            getCallParams: (query) => query,
            formatOutput: (query, transfers) => {
              return extractProductTransfersFromOutputAndTransfers(query, (o) => o.product, transfers).flatMap(
                ({ obj: { latestBlockNumber, product }, range, transfers }) => ({ latestBlockNumber, product, range, transfers }),
              );
            },
          }),
          Rx.concatAll(),

          // split the full range into a list of transfers data so we immediately handle ranges where there is no data to fetch
          Rx.map((item) => {
            const transfersByblockNumber = groupBy(item.transfers, (t) => t.blockNumber);
            const rangesWithTransfers = Object.values(transfersByblockNumber).map((transfers) => ({
              ...item,
              transfers,
              range: { from: transfers[0].blockNumber, to: transfers[0].blockNumber },
            }));
            const rangesWithoutEvents = rangeExcludeMany(
              item.range,
              rangesWithTransfers.flatMap((r) => r.range),
            );

            return rangesWithTransfers.concat(rangesWithoutEvents.map((r) => ({ ...item, transfers: [], range: r })));
          }),

          Rx.concatAll(),
        ),

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
          const vault = item.target.product.productData.vault;
          return {
            shareRateParams: {
              vaultAddress: vault.contract_address,
              underlyingDecimals: vault.want_decimals,
              vaultDecimals: vault.token_decimals,
            },
            balance,
            blockNumber,
            fetchShareRate: true,
            fetchBalance: true,
          };
        } else if (isBeefyBridgedVault(item.target.product)) {
          return {
            balance,
            blockNumber,
            fetchShareRate: false, // we don't have a share rate on the same chain for bridged vaults
            fetchBalance: true,
          };
        } else if (isBeefyBoost(item.target.product)) {
          const boost = item.target.product.productData.boost;
          return {
            shareRateParams: {
              vaultAddress: boost.staked_token_address,
              underlyingDecimals: boost.vault_want_decimals,
              vaultDecimals: boost.staked_token_decimals,
            },
            balance,
            blockNumber,
            fetchShareRate: true,
            fetchBalance: true,
          };
        } else if (isBeefyGovVault(item.target.product)) {
          return {
            balance,
            blockNumber,
            fetchShareRate: false,
            fetchBalance: true,
          };
        }
        logger.error({ msg: "Unsupported product type", data: { product: item.target.product } });
        throw new ProgrammerError("Unsupported product type");
      },
      formatOutput: (item, { balance, blockDatetime, shareRate }) => ({
        ...item,
        blockDatetime,
        balance,
        shareRate,
        shareRateBlockNumber: item.target.transfer.blockNumber,
        shareRateDatetime: blockDatetime,
      }),
    }),

    // fetch bridged vault share rate if needed
    fetchBeefyBridgedVaultShareRateIfNeeded(options),

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
        blockNumber: item.shareRateBlockNumber,
        price: item.shareRate,
        datetime: item.shareRateDatetime,
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

function fetchBeefyBridgedVaultShareRateIfNeeded<TInput extends { target: { product: DbBeefyProduct } }>(options: {
  ctx: ImportCtx;
  emitError: ErrorEmitter<any>;
}): Rx.OperatorFunction<TInput, TInput> {
  if (!getBridgedVaultTargetChains().includes(options.ctx.chain)) {
    return Rx.pipe();
  }
  // fetch bridged vault share rate if needed
  return Rx.connect((items$) =>
    Rx.merge(
      items$.pipe(
        Rx.filter((item) => !isBeefyBridgedVault(item.target.product)),
        Rx.tap((item) => logger.trace({ msg: "not fetching bridged vault share rate", data: { chain: options.ctx.chain, item } })),
      ),
      ...getBridgedVaultOriginChains().map((originChain) =>
        items$.pipe(
          Rx.filter(
            (item): item is any =>
              isBeefyBridgedVault(item.target.product) && item.target.product.productData.vault.bridged_version_of.chain === originChain,
          ),
          createChainPipeline(
            {
              chain: originChain,
              client: options.ctx.client,
              behaviour: options.ctx.behaviour,
            },
            (ctx) =>
              Rx.pipe(
                Rx.tap((item) => logger.trace({ msg: "loading share rate", data: { chain: ctx.chain, transferData: item } })),

                // first we need to map the vault transfer block in the bridged chain to a block in the origin chain
                fetchBlockFromDatetime$({
                  ctx,
                  emitError: options.emitError,
                  getBlockDate: (item) => item.blockDatetime,
                  formatOutput: (item, shareRateBlockNumberOnOriginChain) => ({ ...item, shareRateBlockNumberOnOriginChain }),
                }),

                Rx.tap((item) => logger.trace({ msg: "loading share rate", data: { chain: ctx.chain, transferData: item } })),

                // then we can fetch the share rate in the context of the origin chain
                fetchBeefyTransferData$({
                  ctx,
                  emitError: options.emitError,
                  getCallParams: (item) => {
                    const balance = {
                      decimals: item.target.transfer.tokenDecimals,
                      contractAddress: item.target.transfer.tokenAddress,
                      ownerAddress: item.target.transfer.ownerAddress,
                    };
                    const blockNumber = item.shareRateBlockNumberOnOriginChain;

                    if (!isBeefyBridgedVault(item.target.product)) {
                      logger.error({ msg: "Unsupported product type", data: { product: item.target.product } });
                      throw new ProgrammerError("Unsupported product type");
                    }

                    const vault = item.target.product.productData.vault.bridged_version_of;
                    return {
                      shareRateParams: {
                        vaultAddress: vault.contract_address,
                        underlyingDecimals: vault.want_decimals,
                        vaultDecimals: vault.token_decimals,
                      },
                      balance,
                      blockNumber,
                      fetchShareRate: true, // we don't have a share rate on the same chain for bridged vaults
                      fetchBalance: false,
                    };
                  },
                  formatOutput: (item, { blockDatetime, shareRate }) => ({
                    ...item,
                    shareRate,
                    shareRateBlockNumber: item.shareRateBlockNumber,
                    shareRateDatetime: blockDatetime,
                  }),
                }),
              ),
          ),
        ),
      ),
    ),
  );
}
