import Decimal from "decimal.js";
import { get } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { RpcConfig } from "../../../../types/rpc-config";
import { normalizeAddress } from "../../../../utils/ethers";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { createObservableWithNext } from "../../../../utils/rxjs/utils/create-observable-with-next";
import { ERC20Transfer, fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../../common/connector/erc20-transfers";
import { DbBeefyProduct } from "../../../common/loader/product";
import { ErrorEmitter, ImportQuery, ImportResult } from "../../../common/types/import-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { fetchBeefyPPFS$ } from "../../connector/ppfs";
import { isBeefyBoostProductImportQuery, isBeefyGovVaultProductImportQuery, isBeefyStandardVaultProductImportQuery } from "../../utils/type-guard";
import { loadTransfers$, TransferWithRate } from "./load-transfers";

const logger = rootLogger.child({ module: "beefy", component: "import-product-block-range" });

export function importProductBlockRange$(options: {
  client: PoolClient;
  rpcConfig: RpcConfig;
  chain: Chain;
  emitErrors: ErrorEmitter<DbBeefyProduct, number>;
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
        const boost = item.target.productData.boost;
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
        const boost = item.target.productData.boost;
        return {
          vaultAddress: boost.staked_token_address,
          underlyingDecimals: item.target.productData.boost.vault_want_decimals,
          vaultDecimals: boost.staked_token_decimals,
          blockNumbers: item.transfers.map((t) => t.blockNumber),
        };
      },
      emitErrors: options.emitErrors,
      formatOutput: (item, ppfss) => ({ ...item, ppfss }),
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
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        const vault = item.target.productData.vault;
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
          vaultAddress: item.target.productData.vault.contract_address,
          underlyingDecimals: item.target.productData.vault.want_decimals,
          vaultDecimals: item.target.productData.vault.token_decimals,
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
        normalizeAddress(item.target.productData.vault.contract_address),
      ],
    })),

    fetchERC20TransferToAStakingContract$({
      rpcConfig: options.rpcConfig,
      chain: options.chain,
      streamConfig: options.streamConfig,
      getQueryParams: (item) => {
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        const vault = item.target.productData.vault;
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

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: ImportQuery<DbBeefyProduct, number>) => true),

    // create groups of products to import
    Rx.groupBy((item) =>
      item.target.productData.type !== "beefy:vault" ? "boost" : item.target.productData.vault.is_gov_vault ? "gov-vault" : "standard-vault",
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
          (transfer, idx): ImportQuery<TransferWithRate, number> => ({
            target: {
              transfer,
              product: item.target,
              sharesRate: item.ppfss[idx],
              ignoreAddresses: item.ignoreAddresses,
            },
            range: item.range,
            latest: item.latest,
          }),
        ),
      );

      const {
        observable: transferErrors$,
        next: emitTransferErrors,
        complete: completeTransferErrors$,
      } = createObservableWithNext<ImportQuery<TransferWithRate, number>>();

      return Rx.of(itemsTransfers.flat()).pipe(
        Rx.mergeAll(),

        Rx.tap((item) =>
          logger.trace({
            msg: "importing transfer",
            data: { product: item.target.product.productId, blockRange: item.range, transfer: item.target },
          }),
        ),

        // enhance transfer and insert in database
        loadTransfers$({
          chain: options.chain,
          client: options.client,
          streamConfig: options.streamConfig,
          emitErrors: emitTransferErrors,
          rpcConfig: options.rpcConfig,
        }),

        // merge errors
        Rx.pipe(
          Rx.map((item) => ({ ...item, success: get(item, "success", true) })),
          // make sure we close the errors observable when we are done
          Rx.finalize(() => setTimeout(completeTransferErrors$)),
          // merge the errors back in, all items here should have been successfully treated
          Rx.mergeWith(transferErrors$.pipe(Rx.map((item) => ({ ...item, success: false })))),
          // make sure the type is correct
          Rx.map((item): ImportResult<TransferWithRate, number> => item),
        ),

        // return to product representation
        Rx.toArray(),
        Rx.map((transfers) => {
          return items.map((item) => {
            // get all transfers for this product
            const itemTransferResults = transfers.filter((t) => t.target.product.productId === item.target.productId);
            // only a success if ALL transfer imports were successful
            const success = itemTransferResults.every((t) => t.success);
            return {
              ...item,
              success,
            };
          });
        }),
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
