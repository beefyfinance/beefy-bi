import { PoolClient } from "pg";
import { Chain } from "../../../../types/chain";
import { DbBeefyProduct, DbProduct } from "../../../common/loader/product";
import * as Rx from "rxjs";
import { ERC20Transfer } from "../../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../../common/connector/owner-balance";
import { fetchBlockDatetime$ } from "../../../common/connector/block-datetime";
import { upsertInvestor$ } from "../../../common/loader/investor";
import { upsertInvestment$ } from "../../../common/loader/investment";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../../utils/ethers";
import { rootLogger } from "../../../../utils/logger";
import { ErrorEmitter, ImportQuery } from "../../../common/types/import-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { RpcConfig } from "../../../../types/rpc-config";
import { upsertPrice$ } from "../../../common/loader/prices";

const logger = rootLogger.child({ module: "beefy", component: "import-transfer" });

export type TransferWithRate = ImportQuery<DbProduct> & {
  transfer: ERC20Transfer;
  ignoreAddresses: string[];
  sharesRate: Decimal;
};

export type TransferLoadStatus = { transferCount: number; success: true };

export function loadTransfers$(options: {
  chain: Chain;
  client: PoolClient;
  emitErrors: ErrorEmitter<DbBeefyProduct>;
  streamConfig: BatchStreamConfig;
  rpcConfig: RpcConfig;
}) {
  return Rx.pipe(
    // remove ignored addresses
    Rx.filter((item: TransferWithRate) => {
      const shouldIgnore = item.ignoreAddresses.some((ignoreAddr) => ignoreAddr === normalizeAddress(item.transfer.ownerAddress));
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
        blockNumber: item.transfer.blockNumber,
        decimals: item.transfer.tokenDecimals,
        contractAddress: item.transfer.tokenAddress,
        ownerAddress: item.transfer.ownerAddress,
      }),
      emitErrors: options.emitErrors,
      streamConfig: options.streamConfig,
      formatOutput: (item, vaultSharesBalance) => ({ ...item, vaultSharesBalance }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      rpcConfig: options.rpcConfig,
      getBlockNumber: (t) => t.transfer.blockNumber,
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
      getInvestorData: (item) => ({
        address: item.transfer.ownerAddress,
        investorData: {},
      }),
      formatOutput: (transferData, investorId) => ({ ...transferData, investorId }),
    }),

    // insert ppfs as a price
    upsertPrice$({
      client: options.client,
      getPriceData: (item) => ({
        priceFeedId: item.target.priceFeedId1,
        blockNumber: item.transfer.blockNumber,
        price: item.sharesRate,
        datetime: item.blockDatetime,
        priceData: {
          chain: options.chain,
          trxHash: item.transfer.transactionHash,
          sharesRate: item.sharesRate.toString(),
          productType:
            item.target.productData.type === "beefy:vault"
              ? item.target.productData.type + (item.target.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.target.productData.type,
        },
      }),
      formatOutput: (transferData, priceRow) => ({ ...transferData, priceRow }),
    }),

    // insert the investment data
    upsertInvestment$({
      client: options.client,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.transfer.blockNumber,
        productId: item.target.productId,
        investorId: item.investorId,
        // balance is expressed in vault shares
        balance: item.vaultSharesBalance,
        investmentData: {
          chain: options.chain,
          balance: item.vaultSharesBalance.toString(),
          balanceDiff: item.transfer.amountTransfered.toString(),
          trxHash: item.transfer.transactionHash,
          sharesRate: item.sharesRate.toString(),
          productType:
            item.target.productData.type === "beefy:vault"
              ? item.target.productData.type + (item.target.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.target.productData.type,
        },
      }),
      formatOutput: (transferData, investment) => ({ ...transferData, investment }),
    }),
  );
}
