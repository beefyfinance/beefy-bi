import Decimal from "decimal.js";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { RpcConfig } from "../../../../types/rpc-config";
import { normalizeAddress } from "../../../../utils/ethers";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { ERC20Transfer } from "../../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct, DbProduct } from "../../../common/loader/product";
import { ErrorEmitter, ImportQuery } from "../../../common/types/import-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { isBeefyBoost, isBeefyGovVault, isBeefyStandardVault } from "../../utils/type-guard";

export type TransferToLoad<TProduct extends DbBeefyProduct> = {
  transfer: ERC20Transfer;
  product: TProduct;
  ignoreAddresses: string[];
};

export type TransferLoadStatus = { transferCount: number; success: true };

export function loadTransfers$(options: {
  chain: Chain;
  client: PoolClient;
  emitErrors: ErrorEmitter<TransferToLoad<DbBeefyProduct>, number>;
  streamConfig: BatchStreamConfig;
  rpcConfig: RpcConfig;
}) {
  const govVaultPipeline$ = Rx.pipe(
    // fix typings
    Rx.filter((item): item is ImportQuery<TransferToLoad<DbBeefyGovVaultProduct>, number> => true),
    // simulate a ppfs of 1 so we can treat gov vaults like standard vaults
    Rx.map((item) => ({ ...item, ppfs: new Decimal(1) })),
  );

  const stdVaultOrBoostPipeline$ = Rx.pipe(
    // fix typings
    Rx.filter((item): item is ImportQuery<TransferToLoad<DbBeefyBoostProduct | DbBeefyStdVaultProduct>, number> => true),
    // fetch the ppfs
    fetchBeefyPPFS$({
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
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
      emitErrors: options.emitErrors,
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
    Rx.mergeMap((item) => {
      if (isBeefyBoost(item.target.product)) {
        return Rx.of(item).pipe(stdVaultOrBoostPipeline$);
      } else if (isBeefyStandardVault(item.target.product)) {
        return Rx.of(item).pipe(stdVaultOrBoostPipeline$);
      } else if (isBeefyGovVault(item.target.product)) {
        return Rx.of(item).pipe(govVaultPipeline$);
      } else {
        throw new ProgrammerError(`Unhandled product type`);
      }
    }),

    // we need the balance of each owner
    fetchERC20TokenBalance$({
      chain: options.chain,
      rpcConfig: options.rpcConfig,
      getQueryParams: (item) => ({
        blockNumber: item.target.transfer.blockNumber,
        decimals: item.target.transfer.tokenDecimals,
        contractAddress: item.target.transfer.tokenAddress,
        ownerAddress: item.target.transfer.ownerAddress,
      }),
      emitErrors: options.emitErrors,
      streamConfig: options.streamConfig,
      formatOutput: (item, vaultSharesBalance) => ({ ...item, vaultSharesBalance }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      rpcConfig: options.rpcConfig,
      getBlockNumber: (t) => t.target.transfer.blockNumber,
      emitErrors: options.emitErrors,
      streamConfig: options.streamConfig,
      formatOutput: (item, blockDatetime) => ({ ...item, blockDatetime }),
    }),

    // ==============================
    // now we are ready for the insertion
    // ==============================

    // insert the investor data
    upsertInvestor$({
      client: options.client,
      streamConfig: options.streamConfig,
      emitErrors: options.emitErrors,
      getInvestorData: (item) => ({
        address: item.target.transfer.ownerAddress,
        investorData: {},
      }),
      formatOutput: (transferData, investorId) => ({ ...transferData, investorId }),
    }),

    // insert ppfs as a price
    upsertPrice$({
      client: options.client,
      streamConfig: options.streamConfig,
      emitErrors: options.emitErrors,
      getPriceData: (item) => ({
        priceFeedId: item.target.product.priceFeedId1,
        blockNumber: item.target.transfer.blockNumber,
        price: item.ppfs,
        datetime: item.blockDatetime,
        priceData: {
          chain: options.chain,
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
      client: options.client,
      streamConfig: options.streamConfig,
      emitErrors: options.emitErrors,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.target.transfer.blockNumber,
        productId: item.target.product.productId,
        investorId: item.investorId,
        // balance is expressed in vault shares
        balance: item.vaultSharesBalance,
        investmentData: {
          chain: options.chain,
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
