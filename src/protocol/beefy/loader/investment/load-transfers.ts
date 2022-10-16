import Decimal from "decimal.js";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { RpcConfig } from "../../../../types/rpc-config";
import { normalizeAddress } from "../../../../utils/ethers";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { ERC20Transfer } from "../../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { upsertInvestment$ } from "../../../common/loader/investment";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertPrice$ } from "../../../common/loader/prices";
import { DbProduct } from "../../../common/loader/product";
import { ErrorEmitter, ImportQuery } from "../../../common/types/import-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";

export type TransferWithRate = {
  transfer: ERC20Transfer;
  product: DbProduct;
  ignoreAddresses: string[];
  sharesRate: Decimal;
};

export type TransferLoadStatus = { transferCount: number; success: true };

export function loadTransfers$(options: {
  chain: Chain;
  client: PoolClient;
  emitErrors: ErrorEmitter<TransferWithRate, number>;
  streamConfig: BatchStreamConfig;
  rpcConfig: RpcConfig;
}) {
  return Rx.pipe(
    // remove ignored addresses
    Rx.filter((item: ImportQuery<TransferWithRate, number>) => {
      const shouldIgnore = item.target.ignoreAddresses.some((ignoreAddr) => ignoreAddr === normalizeAddress(item.target.transfer.ownerAddress));
      if (shouldIgnore) {
        //  logger.trace({ msg: "ignoring transfer", data: { chain: options.chain, transferData: item } });
      }
      return !shouldIgnore;
    }),

    // ==============================
    // fetch additional transfer data
    // ==============================

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
        price: item.target.sharesRate,
        datetime: item.blockDatetime,
        priceData: {
          chain: options.chain,
          trxHash: item.target.transfer.transactionHash,
          sharesRate: item.target.sharesRate.toString(),
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
          sharesRate: item.target.sharesRate.toString(),
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
