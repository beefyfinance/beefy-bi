import { PoolClient } from "pg";
import { Chain } from "../../../types/chain";
import { DbProduct } from "../../common/loader/product";
import * as Rx from "rxjs";
import { ERC20Transfer } from "../../common/connector/erc20-transfers";
import { fetchERC20TokenBalance$ } from "../../common/connector/owner-balance";
import { fetchBlockDatetime$ } from "../../common/connector/block-datetime";
import { upsertInvestor$ } from "../../common/loader/investor";
import { upsertInvestment$ } from "../../common/loader/investment";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../utils/ethers";
import { rootLogger } from "../../../utils/logger";
import { ethers } from "ethers";
import { ErrorEmitter, ProductImportQuery } from "../../common/types/product-query";
import { BatchIntakeConfig } from "../../common/utils/batch-rpc-calls";

const logger = rootLogger.child({ module: "common", component: "transfer-loader" });

export type TransferWithRate = ProductImportQuery<DbProduct> & {
  transfer: ERC20Transfer;
  ignoreAddresses: string[];
  sharesRate: Decimal;
};

export type TransferLoadStatus = { transferCount: number; success: true };

export function loadTransfers$(options: {
  chain: Chain;
  client: PoolClient;
  emitErrors: ErrorEmitter;
  intakeConfig: BatchIntakeConfig;
  provider: ethers.providers.JsonRpcProvider;
}): Rx.OperatorFunction<TransferWithRate, TransferLoadStatus> {
  return Rx.pipe(
    // remove ignored addresses
    Rx.filter((item) => {
      const shouldIgnore = item.ignoreAddresses.some((ignoreAddr) => ignoreAddr === normalizeAddress(item.transfer.ownerAddress));
      if (shouldIgnore) {
        //  logger.trace({ msg: "ignoring transfer", data: { chain: options.chain, transferData: item } });
      }
      return !shouldIgnore;
    }),

    Rx.tap((item) =>
      logger.trace({
        msg: "processing transfer data",
        data: {
          chain: options.chain,
          productKey: item.product.productKey,
          transfer: item.transfer,
        },
      }),
    ),

    // ==============================
    // fetch additional transfer data
    // ==============================

    // we need the balance of each owner
    fetchERC20TokenBalance$({
      chain: options.chain,
      provider: options.provider,
      getQueryParams: (item) => ({
        blockNumber: item.transfer.blockNumber,
        decimals: item.transfer.tokenDecimals,
        contractAddress: item.transfer.tokenAddress,
        ownerAddress: item.transfer.ownerAddress,
      }),
      emitErrors: options.emitErrors,
      intakeConfig: options.intakeConfig,
      formatOutput: (item, vaultSharesBalance) => ({ ...item, vaultSharesBalance }),
    }),

    // we also need the date of each block
    fetchBlockDatetime$({
      provider: options.provider,
      getBlockNumber: (t) => t.transfer.blockNumber,
      emitErrors: options.emitErrors,
      intakeConfig: options.intakeConfig,
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

    upsertInvestment$({
      client: options.client,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        productId: item.product.productId,
        investorId: item.investorId,
        // balance is expressed in underlying amount
        balance: item.vaultSharesBalance.mul(item.sharesRate),
        investmentData: {
          blockNumber: item.transfer.blockNumber,
          balance: item.vaultSharesBalance.toString(),
          balanceDiff: item.transfer.amountTransfered.toString(),
          trxHash: item.transfer.transactionHash,
          sharesRate: item.sharesRate.toString(),
          productType:
            item.product.productData.type === "beefy:vault"
              ? item.product.productData.type + (item.product.productData.vault.is_gov_vault ? ":gov" : ":standard")
              : item.product.productData.type,
        },
      }),
      formatOutput: (transferData, investment) => ({ ...transferData, investment }),
    }),

    // only return when we are done with all the transfers
    Rx.count(),

    // return the status of this product ingestion
    Rx.map((transferCount) => ({ transferCount, success: true } as TransferLoadStatus)),
  );
}
