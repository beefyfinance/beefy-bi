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
import { ErrorEmitter, ProductImportQuery } from "../../common/types/product-query";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";
import { RpcConfig } from "../../../types/rpc-config";

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

    upsertInvestment$({
      client: options.client,
      getInvestmentData: (item) => ({
        datetime: item.blockDatetime,
        blockNumber: item.transfer.blockNumber,
        productId: item.product.productId,
        investorId: item.investorId,
        // balance is expressed in underlying amount
        balance: item.vaultSharesBalance.mul(item.sharesRate),
        investmentData: {
          chain: options.chain,
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
  );
}
