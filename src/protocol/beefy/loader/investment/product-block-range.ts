import Decimal from "decimal.js";
import * as Rx from "rxjs";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range } from "../../../../utils/range";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { ERC20Transfer, fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { fetchTransactionGas$ } from "../../../common/connector/transaction-gas";
import { createShouldIgnoreFn } from "../../../common/loader/ignore-address";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery } from "../../../common/types/import-query";
import { executeSubPipeline$ } from "../../../common/utils/execute-sub-pipeline";
import { fetchBeefyBoostTransfers$ } from "../../connector/boost-transfers";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { fetchBeefyPendingRewards$ } from "../../connector/rewards";
import {
  isBeefyBoost,
  isBeefyBoostProductImportQuery,
  isBeefyGovVault,
  isBeefyGovVaultProductImportQuery,
  isBeefyStandardVault,
  isBeefyStandardVaultProductImportQuery,
} from "../../utils/type-guard";
import { upsertInvestorCacheChainInfos$ } from "./investor-cache";

const logger = rootLogger.child({ module: "beefy", component: "import-product-block-range" });

export function importProductBlockRange$<TObj extends ImportRangeQuery<DbBeefyProduct, number>, TErr extends ErrorEmitter<TObj>>(options: {
  ctx: ImportCtx;
  emitBoostError: <T extends ImportRangeQuery<DbBeefyBoostProduct, number>>(obj: T, report: ErrorReport) => void;
  emitStdVaultError: <T extends ImportRangeQuery<DbBeefyStdVaultProduct, number>>(obj: T, report: ErrorReport) => void;
  emitGovVaultError: <T extends ImportRangeQuery<DbBeefyGovVaultProduct, number>>(obj: T, report: ErrorReport) => void;
}) {
  const boostTransfers$ = Rx.pipe(
    Rx.filter(isBeefyBoostProductImportQuery),

    fetchBeefyBoostTransfers$({
      ctx: options.ctx,
      emitError: options.emitBoostError,
      batchAddressesIfPossible: options.ctx.behaviour.mode === "recent",
      getBoostTransfersCallParams: (item) => {
        const boost = item.target.productData.boost;
        return {
          boostAddress: boost.contract_address,
          stakedTokenAddress: boost.staked_token_address,
          stakedTokenDecimals: boost.staked_token_decimals,
          fromBlock: item.range.from,
          toBlock: item.range.to,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),
  );

  const standardVaultTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyStandardVaultProductImportQuery),

    // fetch the vault transfers
    fetchErc20Transfers$({
      ctx: options.ctx,
      emitError: options.emitStdVaultError,
      // we can batch the requests if we are in recent mode
      // since all the query ranges should be the same
      batchAddressesIfPossible: options.ctx.behaviour.mode === "recent",
      getQueryParams: (item) => {
        const vault = item.target.productData.vault;
        return {
          address: vault.contract_address,
          decimals: vault.token_decimals,
          fromBlock: item.range.from,
          toBlock: item.range.to,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),
  );

  const govVaultTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyGovVaultProductImportQuery),

    fetchERC20TransferToAStakingContract$({
      ctx: options.ctx,
      emitError: options.emitGovVaultError,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = item.target.productData.vault;
        return {
          address: vault.want_address,
          decimals: vault.want_decimals,
          trackAddress: vault.contract_address,
          fromBlock: item.range.from,
          toBlock: item.range.to,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),
  );

  const shouldIgnoreFnPromise = createShouldIgnoreFn({ client: options.ctx.client, chain: options.ctx.chain });

  return Rx.pipe(
    // add typings to the input item
    Rx.tap((_: ImportRangeQuery<DbBeefyProduct, number>) => {}),

    // dispatch to all the sub pipelines
    Rx.connect((items$) => Rx.merge(items$.pipe(boostTransfers$), items$.pipe(standardVaultTransfers$), items$.pipe(govVaultTransfers$))),

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
      emitError: (item, report) => {
        if (isBeefyBoostProductImportQuery(item)) {
          options.emitBoostError(item, report);
        } else if (isBeefyStandardVaultProductImportQuery(item)) {
          options.emitStdVaultError(item, report);
        } else if (isBeefyGovVaultProductImportQuery(item)) {
          options.emitGovVaultError(item, report);
        } else {
          throw new ProgrammerError("Unknown product type");
        }
      },
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
  const govVaultPipeline$ = Rx.pipe(
    Rx.filter((item: TInput) => isBeefyGovVault(item.target.product)),
    // simulate a ppfs of 1 so we can treat gov vaults like standard vaults after that
    Rx.map((item) => ({ ...item, ppfs: new Decimal(1) })),

    // find out the user rewards at that block
    //fetchBeefyPendingRewards$({
    //  ctx: options.ctx,
    //  emitError: options.emitError,
    //  getPendingRewardsParams: (item) => {
    //    if (!isBeefyGovVault(item.target.product)) {
    //      throw new ProgrammerError("Expected gov vault");
    //    }
    //    const vault = item.target.product;
    //    if (!vault.productData.vault.gov_vault_reward_token_decimals) {
    //      throw new ProgrammerError("Expected gov vault reward token decimals");
    //    }
    //    return {
    //      blockNumber: item.target.transfer.blockNumber,
    //      tokenDecimals: vault.productData.vault.gov_vault_reward_token_decimals,
    //      contractAddress: vault.productData.vault.contract_address,
    //      ownerAddress: item.target.transfer.ownerAddress,
    //    };
    //  },
    //  formatOutput: (item, pendingRewards) => ({ ...item, pendingRewards }),
    //}),
    Rx.map((item) => ({ ...item, pendingRewards: null })),
  );

  const stdVaultPipeline$ = Rx.pipe(
    Rx.filter((item: TInput) => isBeefyStandardVault(item.target.product)),

    // fetch the ppfs
    fetchBeefyPPFS$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPPFSCallParams: (item) => {
        if (!isBeefyStandardVault(item.target.product)) {
          throw new ProgrammerError("Expected gov vault");
        }
        const vault = item.target.product;
        return {
          vaultAddress: vault.productData.vault.contract_address,
          underlyingDecimals: vault.productData.vault.want_decimals,
          vaultDecimals: vault.productData.vault.token_decimals,
          blockNumber: item.target.transfer.blockNumber,
        };
      },
      formatOutput: (item, ppfs) => ({ ...item, ppfs }),
    }),

    // no rewards for std vaults
    Rx.map((item) => ({ ...item, pendingRewards: null })),
  );

  const boostPipeline$ = Rx.pipe(
    Rx.filter((item: TInput) => isBeefyBoost(item.target.product)),

    // fetch the ppfs
    fetchBeefyPPFS$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPPFSCallParams: (item) => {
        if (!isBeefyBoost(item.target.product)) {
          throw new ProgrammerError("Expected gov vault");
        }
        const boostData = item.target.product.productData.boost;
        return {
          vaultAddress: boostData.staked_token_address,
          underlyingDecimals: boostData.vault_want_decimals,
          vaultDecimals: boostData.staked_token_decimals,
          blockNumber: item.target.transfer.blockNumber,
        };
      },
      formatOutput: (item, ppfs) => ({ ...item, ppfs }),
    }),

    // find out the user rewards at that block
    //fetchBeefyPendingRewards$({
    //  ctx: options.ctx,
    //  emitError: options.emitError,
    //  getPendingRewardsParams: (item) => {
    //    if (!isBeefyBoost(item.target.product)) {
    //      throw new ProgrammerError("Expected boost product");
    //    }
    //    const boost = item.target.product;
    //    return {
    //      blockNumber: item.target.transfer.blockNumber,
    //      tokenDecimals: boost.productData.boost.reward_token_decimals,
    //      contractAddress: boost.productData.boost.contract_address,
    //      ownerAddress: item.target.transfer.ownerAddress,
    //    };
    //  },
    //  formatOutput: (item, pendingRewards) => ({ ...item, pendingRewards }),
    //}),
    Rx.map((item) => ({ ...item, pendingRewards: null })),
  );

  return Rx.pipe(
    Rx.tap((item: TInput) => logger.trace({ msg: "loading transfer", data: { chain: options.ctx.chain, transferData: item } })),

    // ==============================
    // fetch additional transfer data
    // ==============================

    // fetch the ppfs
    Rx.connect((items$) => Rx.merge(items$.pipe(govVaultPipeline$), items$.pipe(stdVaultPipeline$), items$.pipe(boostPipeline$))),

    // we need the balance of each owner
    fetchERC20TokenBalance$({
      ctx: options.ctx,
      emitError: options.emitError,
      getQueryParams: (item) => ({
        blockNumber: item.target.transfer.blockNumber,
        decimals: item.target.transfer.tokenDecimals,
        contractAddress: item.target.transfer.tokenAddress,
        ownerAddress: item.target.transfer.ownerAddress,
      }),
      formatOutput: (item, vaultSharesBalance) => ({ ...item, vaultSharesBalance }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      ctx: options.ctx,
      emitError: options.emitError,
      getBlockNumber: (t) => t.target.transfer.blockNumber,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    // fetch the transaction cost in gas so we can calculate the gas cost of the transfer and ROI/APY better
    // not in use right now
    /*fetchTransactionGas$({
      ctx: options.ctx,
      emitError: options.emitError,
      getQueryParams: (item) => ({
        blockNumber: item.target.transfer.blockNumber,
        transactionHash: item.target.transfer.transactionHash,
      }),
      formatOutput: (item, gas) => ({ ...item, gas }),
    }),*/

    // ==============================
    // now we are ready for the insertion
    // ==============================

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
        price: item.ppfs,
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
        balance: item.vaultSharesBalance,
        balanceDiff: item.target.transfer.amountTransferred,
        pendingRewards: item.pendingRewards,
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
          pendingRewards: item.investment.pendingRewards,
          pendingRewardsDiff: item.investment.pendingRewardsDiff,
          shareToUnderlyingPrice: item.ppfs,
          underlyingBalance: item.investment.balance.mul(item.ppfs),
          underlyingDiff: item.investment.balanceDiff.mul(item.ppfs),
        },
      }),
      formatOutput: (item, investorCacheChainInfos) => ({ ...item, investorCacheChainInfos }),
    }),
  );
}
