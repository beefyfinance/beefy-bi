import Decimal from "decimal.js";
import * as Rx from "rxjs";
import { normalizeAddress } from "../../../../utils/ethers";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { ERC20Transfer } from "../../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ImportCtx } from "../../../common/types/import-context";
import { ImportQuery } from "../../../common/types/import-query";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { isBeefyBoost, isBeefyGovVault, isBeefyStandardVault } from "../../utils/type-guard";

export type TransferToLoad<TProduct extends DbBeefyProduct> = {
  transfer: ERC20Transfer;
  product: TProduct;
  ignoreAddresses: string[];
};

export type TransferLoadStatus = { transferCount: number; success: true };

type TransferPayload<TProduct extends DbBeefyProduct> = ImportQuery<TransferToLoad<TProduct>, number>;

export function loadTransfers$(options: { ctx: ImportCtx<ImportQuery<TransferToLoad<DbBeefyProduct>, number>> }) {
  const govVaultPipeline$ = Rx.pipe(
    // fix typings
    Rx.filter((item: TransferPayload<DbBeefyProduct>): item is TransferPayload<DbBeefyGovVaultProduct> => isBeefyGovVault(item.target.product)),
    // simulate a ppfs of 1 so we can treat gov vaults like standard vaults
    Rx.map((item) => ({ ...item, ppfs: new Decimal(1) })),
  );

  const stdVaultOrBoostPipeline$ = Rx.pipe(
    // fix typings
    Rx.filter(
      (item: TransferPayload<DbBeefyProduct>): item is TransferPayload<DbBeefyBoostProduct | DbBeefyStdVaultProduct> =>
        isBeefyStandardVault(item.target.product) || isBeefyBoost(item.target.product),
    ),
    // fetch the ppfs
    fetchBeefyPPFS$({
      ctx: options.ctx as unknown as ImportCtx<TransferPayload<DbBeefyBoostProduct | DbBeefyStdVaultProduct>>, // ugly AF
      getPPFSCallParams: (item) => {
        if (isBeefyBoost(item.target.product)) {
          const boostData = item.target.product.productData.boost;
          return {
            vaultAddress: boostData.staked_token_address,
            underlyingDecimals: boostData.vault_want_decimals,
            vaultDecimals: boostData.staked_token_decimals,
            blockNumber: item.target.transfer.blockNumber,
          };
        } else {
          const vault = item.target.product;
          return {
            vaultAddress: vault.productData.vault.contract_address,
            underlyingDecimals: vault.productData.vault.want_decimals,
            vaultDecimals: vault.productData.vault.token_decimals,
            blockNumber: item.target.transfer.blockNumber,
          };
        }
      },
      formatOutput: (item, ppfs) => ({ ...item, ppfs }),
    }),
  );

  return Rx.pipe(
    // remove ignored addresses
    Rx.filter((item: ImportQuery<TransferToLoad<DbBeefyProduct>, number>) => {
      const shouldIgnore = item.target.ignoreAddresses.some((ignoreAddr) => ignoreAddr === normalizeAddress(item.target.transfer.ownerAddress));
      if (shouldIgnore) {
        //  logger.trace({ msg: "ignoring transfer", data: { chain: options.chain, transferData: item } });
      }
      return !shouldIgnore;
    }),

    // ==============================
    // fetch additional transfer data
    // ==============================

    // fetch the ppfs
    Rx.connect((items$) => Rx.merge(govVaultPipeline$(items$), stdVaultOrBoostPipeline$(items$))),

    // we need the balance of each owner
    fetchERC20TokenBalance$({
      ctx: options.ctx,
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
      getBlockNumber: (t) => t.target.transfer.blockNumber,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    // ==============================
    // now we are ready for the insertion
    // ==============================

    // insert the investor data
    upsertInvestor$({
      ctx: options.ctx,
      getInvestorData: (item) => ({
        address: item.target.transfer.ownerAddress,
        investorData: {},
      }),
      formatOutput: (transferData, investorId) => ({ ...transferData, investorId }),
    }),

    // insert ppfs as a price
    upsertPrice$({
      ctx: options.ctx,
      getPriceData: (item) => ({
        priceFeedId: item.target.product.priceFeedId1,
        blockNumber: item.target.transfer.blockNumber,
        price: item.ppfs,
        datetime: item.blockDatetime,
        priceData: {
          chain: options.ctx.rpcConfig.chain,
          trxHash: item.target.transfer.transactionHash,
          sharesRate: item.ppfs.toString(),
          productType:
            item.target.product.productData.type === "beefy:vault"
              ? item.target.product.productData.type + (item.target.product.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.target.product.productData.type,
        },
      }),
      formatOutput: (transferData, priceRow) => ({ ...transferData, priceRow }),
    }),

    // insert the investment data
    upsertInvestment$({
      ctx: options.ctx,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.target.transfer.blockNumber,
        productId: item.target.product.productId,
        investorId: item.investorId,
        // balance is expressed in vault shares
        balance: item.vaultSharesBalance,
        investmentData: {
          chain: options.ctx.rpcConfig.chain,
          balance: item.vaultSharesBalance.toString(),
          balanceDiff: item.target.transfer.amountTransfered.toString(),
          trxHash: item.target.transfer.transactionHash,
          sharesRate: item.ppfs.toString(),
          productType:
            item.target.product.productData.type === "beefy:vault"
              ? item.target.product.productData.type + (item.target.product.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.target.product.productData.type,
        },
      }),
      formatOutput: (transferData, investment) => ({ ...transferData, investment }),
    }),
  );
}
