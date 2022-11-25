import Decimal from "decimal.js";
import * as Rx from "rxjs";
import { getAddressBookTokenDecimals } from "../../../../utils/addressbook";
import { normalizeAddress } from "../../../../utils/ethers";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range } from "../../../../utils/range";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { ERC20Transfer, fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { fetchTransactionGas$ } from "../../../common/connector/transaction-gas";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../../common/types/import-context";
import { ImportRangeQuery } from "../../../common/types/import-query";
import { executeSubPipeline$ } from "../../../common/utils/execute-sub-pipeline";
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

const logger = rootLogger.child({ module: "beefy", component: "import-product-block-range" });

export function importProductBlockRange$<TObj extends ImportRangeQuery<DbBeefyProduct, number>, TErr extends ErrorEmitter<TObj>>(options: {
  ctx: ImportCtx;
  emitBoostError: <T extends ImportRangeQuery<DbBeefyBoostProduct, number>>(obj: T, report: ErrorReport) => void;
  emitStdVaultError: <T extends ImportRangeQuery<DbBeefyStdVaultProduct, number>>(obj: T, report: ErrorReport) => void;
  emitGovVaultError: <T extends ImportRangeQuery<DbBeefyGovVaultProduct, number>>(obj: T, report: ErrorReport) => void;
  mode: "recent" | "historical";
}) {
  const boostTransfers$ = Rx.pipe(
    Rx.filter(isBeefyBoostProductImportQuery),

    // fetch latest transfers from and to the boost contract
    fetchERC20TransferToAStakingContract$({
      ctx: options.ctx,
      emitError: options.emitBoostError,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const boost = item.target.productData.boost;
        return {
          address: boost.staked_token_address,
          decimals: boost.staked_token_decimals,
          trackAddress: boost.contract_address,
          fromBlock: item.range.from,
          toBlock: item.range.to,
        };
      },
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // add an ignore address so we can pipe this observable into the main pipeline again
    Rx.map((item) => ({ ...item, ignoreAddresses: [item.target.productData.boost.contract_address] })),
  );

  const standardVaultTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyStandardVaultProductImportQuery),

    // for standard vaults, we only ignore the mint-burn addresses
    Rx.map((item) => ({
      ...item,
      ignoreAddresses: [normalizeAddress("0x0000000000000000000000000000000000000000")],
    })),

    // fetch the vault transfers
    fetchErc20Transfers$({
      ctx: options.ctx,
      emitError: options.emitStdVaultError,
      // we only want to fetch from etherscan when fetching historical data
      // for recent data we are better off using rpc batching since we fetch a small amount of data for many addresses
      // whereas for historical data we are able to batch lots of ranges for a single address in one etherscan call
      allowFetchingFromEtherscan: options.mode === "historical",
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

    // for gov vaults, we ignore the vault address and the associated maxi vault to avoid double counting
    // todo: ignore the maxi vault
    Rx.map((item) => ({
      ...item,
      ignoreAddresses: [
        normalizeAddress("0x0000000000000000000000000000000000000000"),
        normalizeAddress(item.target.productData.vault.contract_address),
      ],
    })),

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
      getObjs: (item) =>
        item.transfers
          .map(
            (transfer): TransferToLoad => ({
              transfer,
              product: item.target,
              latest: item.latest,
              range: item.range,
            }),
          )
          .filter((transfer) => {
            const shouldIgnore = item.ignoreAddresses.some((ignoreAddr) => ignoreAddr === normalizeAddress(transfer.transfer.ownerAddress));
            if (shouldIgnore) {
              //  logger.trace({ msg: "ignoring transfer", data: { chain: options.chain, transferData: item } });
            }
            return !shouldIgnore;
          }),
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
    fetchBeefyPendingRewards$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPendingRewardsParams: (item) => {
        if (!isBeefyGovVault(item.target.product)) {
          throw new ProgrammerError("Expected gov vault");
        }
        const vault = item.target.product;
        if (!vault.productData.vault.gov_vault_reward_token_decimals) {
          throw new ProgrammerError("Expected gov vault reward token decimals");
        }
        return {
          blockNumber: item.target.transfer.blockNumber,
          tokenDecimals: vault.productData.vault.gov_vault_reward_token_decimals,
          contractAddress: vault.productData.vault.contract_address,
          ownerAddress: item.target.transfer.ownerAddress,
        };
      },
      formatOutput: (item, pendingRewards) => ({ ...item, pendingRewards }),
    }),
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
    fetchBeefyPendingRewards$({
      ctx: options.ctx,
      emitError: options.emitError,
      getPendingRewardsParams: (item) => {
        if (!isBeefyBoost(item.target.product)) {
          throw new ProgrammerError("Expected boost product");
        }
        const boost = item.target.product;
        return {
          blockNumber: item.target.transfer.blockNumber,
          tokenDecimals: boost.productData.boost.reward_token_decimals,
          contractAddress: boost.productData.boost.contract_address,
          ownerAddress: item.target.transfer.ownerAddress,
        };
      },
      formatOutput: (item, pendingRewards) => ({ ...item, pendingRewards }),
    }),
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
    fetchTransactionGas$({
      ctx: options.ctx,
      emitError: options.emitError,
      getQueryParams: (item) => ({
        blockNumber: item.target.transfer.blockNumber,
        transactionHash: item.target.transfer.transactionHash,
      }),
      formatOutput: (item, gas) => ({ ...item, gas }),
    }),

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
        priceData: {
          chain: options.ctx.chain,
          trxHash: item.target.transfer.transactionHash,
          sharesRate: item.ppfs.toString(),
          productType:
            item.target.product.productData.type === "beefy:vault"
              ? item.target.product.productData.type + (item.target.product.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.target.product.productData.type,
          query: { range: item.target.range, latest: item.target.latest, date: new Date().toISOString() },
        },
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
        // balance is expressed in vault shares
        balance: item.vaultSharesBalance,
        balanceDiff: item.target.transfer.amountTransferred,
        pendingRewards: item.pendingRewards,
        pendingRewardsDiff: null,
        investmentData: {
          chain: options.ctx.chain,
          balance: item.vaultSharesBalance.toString(),
          balanceDiff: item.target.transfer.amountTransferred.toString(),
          trxHash: item.target.transfer.transactionHash,
          sharesRate: item.ppfs.toString(),
          productType:
            item.target.product.productData.type === "beefy:vault"
              ? item.target.product.productData.type + (item.target.product.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.target.product.productData.type,
          query: { range: item.target.range, latest: item.target.latest },
          gas: {
            cumulativeGasUsed: item.gas.cumulativeGasUsed.toString(),
            effectiveGasPrice: item.gas.effectiveGasPrice.toString(),
            gasUsed: item.gas.gasUsed.toString(),
            l1Fee: item.gas.l1Fee?.toString(),
            l1FeeScalar: item.gas.l1FeeScalar?.toString(),
            l1GasPrice: item.gas.l1GasPrice?.toString(),
            l1GasUsed: item.gas.l1GasUsed?.toString(),
          },
          importDate: new Date().toISOString(),
          lineage: { log: item.target.transfer.logLineage },
        },
      }),
      formatOutput: (item, investment) => ({ ...item, investment, result: true }),
    }),
  );
}
