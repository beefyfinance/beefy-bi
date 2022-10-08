import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { PoolClient } from "pg";
import { fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../../common/connector/erc20-transfers";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../../utils/ethers";
import { DbBeefyProduct } from "../../../common/loader/product";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { loadTransfers$, TransferWithRate } from "./load-transfers";
import { ErrorEmitter, ProductImportQuery } from "../../../common/types/product-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { isBeefyBoostProductImportQuery, isBeefyGovVaultProductImportQuery, isBeefyStandardVaultProductImportQuery } from "../../utils/type-guard";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { RpcConfig } from "../../../../types/rpc-config";
import { rootLogger } from "../../../../utils/logger";

const logger = rootLogger.child({ module: "beefy", component: "import-product-block-range" });

export function importProductBlockRange$(options: {
  client: PoolClient;
  rpcConfig: RpcConfig;
  chain: Chain;
  emitErrors: ErrorEmitter;
  streamConfig: BatchStreamConfig;
}) {
  const boostTransfers$ = Rx.pipe(
    // set the right product type
    Rx.filter(isBeefyBoostProductImportQuery),

    // fetch latest transfers from and to the boost contract
    fetchERC20TransferToAStakingContract$({
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const boost = item.product.productData.boost;
        return {
          address: boost.staked_token_address,
          decimals: boost.staked_token_decimals,
          trackAddress: boost.contract_address,
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // then we need the vault ppfs to interpret the balance
    fetchBeefyPPFS$({
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getPPFSCallParams: (item) => {
        const boost = item.product.productData.boost;
        return {
          vaultAddress: boost.staked_token_address,
          underlyingDecimals: item.product.productData.boost.vault_want_decimals,
          vaultDecimals: boost.staked_token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
    }),

    // add an ignore address so we can pipe this observable into the main pipeline again
    Rx.map((item) => ({ ...item, ignoreAddresses: [item.product.productData.boost.contract_address] })),
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
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        const vault = item.product.productData.vault;
        return {
          address: vault.contract_address,
          decimals: vault.token_decimals,
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // fetch the ppfs
    fetchBeefyPPFS$({
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getPPFSCallParams: (item) => {
        return {
          vaultAddress: item.product.productData.vault.contract_address,
          underlyingDecimals: item.product.productData.vault.want_decimals,
          vaultDecimals: item.product.productData.vault.token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
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
        normalizeAddress(item.product.productData.vault.contract_address),
      ],
    })),

    fetchERC20TransferToAStakingContract$({
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = item.product.productData.vault;
        return {
          address: vault.want_address,
          decimals: vault.want_decimals,
          trackAddress: vault.contract_address,
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, transfers) => ({ ...item, transfers }),
    }),

    // simulate a ppfs of 1 so we can treat gov vaults like standard vaults
    Rx.map((item) => ({
      ...item,
      ppfss: item.transfers.map(() => new Decimal(1)),
    })),
  );

  const importTransfers$ = loadTransfers$({
    chain: options.chain,
    client: options.client,
    streamConfig: options.streamConfig,
    emitErrors: options.emitErrors,
    rpcConfig: options.rpcConfig,
  });

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: ProductImportQuery<DbBeefyProduct>) => true),

    // create groups of products to import
    Rx.groupBy((item) =>
      item.product.productData.type !== "beefy:vault" ? "boost" : item.product.productData.vault.is_gov_vault ? "gov-vault" : "standard-vault",
    ),
    Rx.mergeMap((productType$) => {
      if (productType$.key === "boost") {
        return productType$.pipe(boostTransfers$);
      } else if (productType$.key === "standard-vault") {
        return productType$.pipe(standardVaultTransfers$);
      } else if (productType$.key === "gov-vault") {
        return productType$.pipe(govVaultTransfers$);
      } else {
        throw new ProgrammerError(`Unhandled product type: ${productType$.key}`);
      }
    }),

    Rx.tap((item) => {
      if (item.transfers.length > 0) {
        logger.debug({
          msg: "Got transfers for product",
          data: { productId: item.product.productId, blockRange: item.blockRange, transferCount: item.transfers.length },
        });

        // add some verification about the transfers
        if (process.env.NODE_ENV === "development") {
          for (const transfer of item.transfers) {
            if (transfer.blockNumber < item.blockRange.from || transfer.blockNumber > item.blockRange.to) {
              logger.error({
                msg: "Transfer out of requested block range",
                data: { productId: item.product.productId, blockRange: item.blockRange, transferBlock: transfer.blockNumber, transfer },
              });
            }
          }
        }
      }
    }),

    // work by batches of 300 block range
    Rx.bufferTime(options.streamConfig.maxInputWaitMs, undefined, 300),

    Rx.tap((items) =>
      logger.debug({
        msg: "Importing a batch of transfers",
        data: { count: items.length, transferLen: items.map((i) => i.transfers.length).reduce((a, b) => a + b, 0) },
      }),
    ),

    Rx.mergeMap((items) => {
      const itemsTransfers = items.map((item) =>
        item.transfers.map(
          (transfer, idx): TransferWithRate => ({
            transfer,
            ignoreAddresses: item.ignoreAddresses,
            product: item.product,
            sharesRate: item.ppfss[idx],
            blockRange: item.blockRange,
            latestBlockNumber: item.latestBlockNumber,
          }),
        ),
      );

      return Rx.of(itemsTransfers.flat()).pipe(
        Rx.mergeAll(),

        Rx.tap((item) =>
          logger.trace({
            msg: "importing transfer",
            data: { product: item.product.productId, blockRange: item.blockRange, transfer: item.transfer },
          }),
        ),

        // enhance transfer and insert in database
        importTransfers$,

        // return to product representation
        Rx.count(),
        Rx.map(() => items.map((item) => ({ ...item, success: true }))),
      );
    }, options.streamConfig.workConcurrency),

    Rx.tap((items) =>
      logger.debug({
        msg: "Done importing a transfer batch",
        data: { count: items.length },
      }),
    ),

    Rx.mergeAll(), // flatten items
  );
}
