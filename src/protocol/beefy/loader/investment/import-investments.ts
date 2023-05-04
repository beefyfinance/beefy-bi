import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { mergeLogsInfos, rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range } from "../../../../utils/range";
import { excludeNullFields$ } from "../../../../utils/rxjs/utils/exclude-null-field";
import { fetchContractCreationInfos$ } from "../../../common/connector/contract-creation";
import { ERC20Transfer, fetchERC20TransferToAStakingContract$, fetchErc20Transfers$ } from "../../../common/connector/erc20-transfers";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../../common/connector/import-queries";
import { upsertBlock$ } from "../../../common/loader/blocks";
import { createShouldIgnoreFn } from "../../../common/loader/ignore-address";
import { DbProductInvestmentImportState } from "../../../common/loader/import-state";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery } from "../../../common/types/import-query";
import { isProductDashboardEOL } from "../../../common/utils/eol";
import { executeSubPipeline$ } from "../../../common/utils/execute-sub-pipeline";
import { createHistoricalImportRunner, createRecentImportRunner } from "../../../common/utils/historical-recent-pipeline";
import { ChainRunnerConfig } from "../../../common/utils/rpc-chain-runner";
import { fetchBeefyTransferData$ } from "../../connector/transfer-data";
import { getProductContractAddress } from "../../utils/contract-accessors";
import { getInvestmentsImportStateKey } from "../../utils/import-state";
import {
  isBeefyBoost,
  isBeefyBoostProductImportQuery,
  isBeefyGovVault,
  isBeefyGovVaultOrBoostProductImportQuery,
  isBeefyGovVaultProductImportQuery,
  isBeefyStandardVault,
  isBeefyStandardVaultProductImportQuery,
} from "../../utils/type-guard";
import { upsertInvestorCacheChainInfos$ } from "./investor-cache";

const logger = rootLogger.child({ module: "beefy", component: "investment-import" });

export function createBeefyHistoricalInvestmentRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbBeefyProduct> }) {
  return createHistoricalImportRunner<DbBeefyProduct, number, DbProductInvestmentImportState>({
    runnerConfig: options.runnerConfig,
    logInfos: { msg: "Importing historical beefy investments", data: { chain: options.chain } },
    getImportStateKey: getInvestmentsImportStateKey,
    isLiveItem: (p) => !isProductDashboardEOL(p),
    generateQueries$: (ctx, emitError) =>
      Rx.pipe(
        addHistoricalBlockQuery$({
          ctx,
          emitError,
          isLiveItem: (p) => !isProductDashboardEOL(p.target),
          getImport: (item) => item.importState,
          getFirstBlockNumber: (importState) => importState.importData.contractCreatedAtBlock,
          formatOutput: (item, latestBlockNumber, blockQueries) => blockQueries.map((range) => ({ ...item, range, latest: latestBlockNumber })),
        }),
        Rx.concatAll(),
      ),
    createDefaultImportState$: (ctx) =>
      Rx.pipe(
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
    processImportQuery$: (ctx, emitError) => importProductBlockRange$({ ctx, emitError }),
  });
}

export function createBeefyRecentInvestmentRunner(options: { chain: Chain; runnerConfig: ChainRunnerConfig<DbBeefyProduct> }) {
  return createRecentImportRunner<DbBeefyProduct, number, DbProductInvestmentImportState>({
    runnerConfig: options.runnerConfig,
    cacheKey: "beefy:product:investment:recent",
    logInfos: { msg: "Importing recent beefy investments", data: { chain: options.chain } },
    getImportStateKey: getInvestmentsImportStateKey,
    isLiveItem: (p) => !isProductDashboardEOL(p),
    generateQueries$: ({ ctx, emitError, lastImported, formatOutput }) =>
      addLatestBlockQuery$({
        ctx,
        emitError,
        getLastImportedBlock: () => lastImported,
        formatOutput: (item, latest, range) => formatOutput(item, latest, [range]),
      }),
    processImportQuery$: (ctx, emitError) => importProductBlockRange$({ ctx, emitError }),
  });
}

export function importProductBlockRange$(options: {
  ctx: ImportCtx;
  emitError: <T extends ImportRangeQuery<DbBeefyProduct, number>>(obj: T, report: ErrorReport) => void;
}) {
  const shouldIgnoreFnPromise = createShouldIgnoreFn({ client: options.ctx.client, chain: options.ctx.chain });

  return Rx.pipe(
    // add typings to the input item
    Rx.tap((_: ImportRangeQuery<DbBeefyProduct, number>) => {}),

    // dispatch to all the sub pipelines
    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          // set the right product type
          Rx.filter(isBeefyGovVaultOrBoostProductImportQuery),

          fetchERC20TransferToAStakingContract$({
            ctx: options.ctx,
            emitError: options.emitError,
            getQueryParams: (item) => {
              if (isBeefyBoost(item.target)) {
                const boost = item.target.productData.boost;
                return {
                  tokenAddress: boost.staked_token_address,
                  decimals: boost.staked_token_decimals,
                  trackAddress: boost.contract_address,
                  fromBlock: item.range.from,
                  toBlock: item.range.to,
                };
              } else if (isBeefyGovVault(item.target)) {
                // for gov vaults we don't have a share token so we use the underlying token
                // transfers and filter on those transfer from and to the contract address
                const vault = item.target.productData.vault;
                return {
                  tokenAddress: vault.want_address,
                  decimals: vault.want_decimals,
                  trackAddress: vault.contract_address,
                  fromBlock: item.range.from,
                  toBlock: item.range.to,
                };
              } else {
                throw new ProgrammerError({ msg: "Invalid product type, should be gov vault or boost", data: { product: item.target } });
              }
            },
            formatOutput: (item, transfers) => ({ ...item, transfers }),
          }),
        ),
        items$.pipe(
          // set the right product type
          Rx.filter(isBeefyStandardVaultProductImportQuery),

          // fetch the vault transfers
          fetchErc20Transfers$({
            ctx: options.ctx,
            emitError: options.emitError,
            // we can batch the requests if we are in recent mode
            // since all the query ranges should be the same
            batchAddressesIfPossible: options.ctx.behaviour.mode === "recent",
            getQueryParams: (item) => {
              const vault = item.target.productData.vault;
              return {
                tokenAddress: vault.contract_address,
                decimals: vault.token_decimals,
                fromBlock: item.range.from,
                toBlock: item.range.to,
              };
            },
            formatOutput: (item, transfers) => ({ ...item, transfers }),
          }),
        ),
      ),
    ),

    Rx.tap((item) => {
      if (item.transfers.length > 0) {
        logger.debug({
          msg: "Got transfers for product",
          data: { productId: item.target.productId, blockRange: item.range, transferCount: item.transfers.length },
        });

        // add some verification about the transfers
        if (process.env.NODE_ENV === "development") {
          for (const transfer of item.transfers) {
            if (transfer.blockNumber < item.range.from || transfer.blockNumber > item.range.to) {
              logger.error({
                msg: "Transfer out of requested block range",
                data: { productId: item.target.productId, blockRange: item.range, transferBlock: transfer.blockNumber, transfer },
              });
            }
          }
        }
      }
    }),

    executeSubPipeline$({
      ctx: options.ctx,
      emitError: options.emitError,
      getObjs: async (item) => {
        const shouldIgnoreFn = await shouldIgnoreFnPromise;
        return item.transfers
          .map(
            (transfer): TransferToLoad => ({
              transfer,
              product: item.target,
              latest: item.latest,
              range: item.range,
            }),
          )
          .filter((transfer) => {
            const shouldIgnore = shouldIgnoreFn(transfer.transfer.ownerAddress);
            if (shouldIgnore) {
              logger.trace({ msg: "ignoring transfer", data: { chain: options.ctx.chain, transferData: item } });
            } else {
              logger.trace({ msg: "not ignoring transfer", data: { chain: options.ctx.chain, ownerAddress: transfer.transfer.ownerAddress } });
            }
            return !shouldIgnore;
          });
      },
      pipeline: (emitError) => loadTransfers$({ ctx: options.ctx, emitError }),
      formatOutput: (item, _ /* we don't care about the result */) => item,
    }),

    Rx.map((item) => ({ ...item, success: true })),
  );
}

export type TransferToLoad<TProduct extends DbBeefyProduct = DbBeefyProduct> = {
  transfer: ERC20Transfer;
  product: TProduct;
  range: Range<number>;
  latest: number;
};

export type TransferLoadStatus = { transferCount: number; success: true };

export function loadTransfers$<
  TObj,
  TInput extends { parent: TObj; target: TransferToLoad<DbBeefyProduct> },
  TErr extends ErrorEmitter<TInput>,
>(options: { ctx: ImportCtx; emitError: TErr }) {
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
