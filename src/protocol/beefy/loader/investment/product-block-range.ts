import { get } from "lodash";
import * as Rx from "rxjs";
import { normalizeAddress } from "../../../../utils/ethers";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { createObservableWithNext } from "../../../../utils/rxjs/utils/create-observable-with-next";
import { fetchErc20Transfers$, fetchERC20TransferToAStakingContract$ } from "../../../common/connector/erc20-transfers";
import { DbBeefyBoostProduct, DbBeefyGovVaultProduct, DbBeefyProduct, DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ImportCtx } from "../../../common/types/import-context";
import { ImportQuery, ImportResult } from "../../../common/types/import-query";
import { isBeefyBoostProductImportQuery, isBeefyGovVaultProductImportQuery, isBeefyStandardVaultProductImportQuery } from "../../utils/type-guard";
import { loadTransfers$, TransferToLoad } from "./load-transfers";

const logger = rootLogger.child({ module: "beefy", component: "import-product-block-range" });

export function importProductBlockRange$<TObj extends ImportQuery<DbBeefyProduct, number>, TCtx extends ImportCtx<TObj>>(options: { ctx: TCtx }) {
  const boostTransfers$ = Rx.pipe(
    Rx.filter(isBeefyBoostProductImportQuery),

    // fetch latest transfers from and to the boost contract
    fetchERC20TransferToAStakingContract$({
      ctx: options.ctx as unknown as ImportCtx<ImportQuery<DbBeefyBoostProduct, number>>,
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
      ctx: options.ctx as unknown as ImportCtx<ImportQuery<DbBeefyStdVaultProduct, number>>,
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
      ctx: options.ctx as unknown as ImportCtx<ImportQuery<DbBeefyGovVaultProduct, number>>,
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
    Rx.pipe(
      Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, 300),
      Rx.filter((items) => items.length > 0),

      Rx.tap((items) =>
        logger.debug({
          msg: "Importing a batch of transfers",
          data: { count: items.length, transferLen: items.map((i) => i.transfers.length).reduce((a, b) => a + b, 0) },
        }),
      ),
    ),

    Rx.mergeMap((items) => {
      const itemsTransfers = items.map((item) =>
        item.transfers.map(
          (transfer): ImportQuery<TransferToLoad<DbBeefyProduct>, number> => ({
            target: {
              transfer,
              product: item.target,
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
      } = createObservableWithNext<ImportQuery<TransferToLoad<DbBeefyProduct>, number>>();

      const transfersCtx = {
        ...options.ctx,
        emitErrors: emitTransferErrors,
      };

      return Rx.of(itemsTransfers.flat()).pipe(
        Rx.mergeAll(),

        Rx.tap((item) =>
          logger.trace({
            msg: "importing transfer",
            data: { product: item.target.product.productId, blockRange: item.range, transfer: item.target },
          }),
        ),

        // enhance transfer and insert in database
        loadTransfers$({ ctx: transfersCtx }),

        // merge errors
        Rx.pipe(
          Rx.map((item) => ({ ...item, success: get(item, "success", true) })),
          // make sure we close the errors observable when we are done
          Rx.finalize(() => setTimeout(completeTransferErrors$)),
          // merge the errors back in, all items here should have been successfully treated
          Rx.mergeWith(transferErrors$.pipe(Rx.map((item) => ({ ...item, success: false })))),
          // make sure the type is correct
          Rx.map((item): ImportResult<TransferToLoad<DbBeefyProduct>, number> => item),
        ),

        // return to product representation
        Rx.toArray(),
        Rx.map((transfers) => {
          logger.trace({ msg: "imported transfers", data: { itemCount: items.length, transferCount: transfers.length } });
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
    }, options.ctx.streamConfig.workConcurrency),

    Rx.mergeAll(), // flatten items

    // mark the success status
    Rx.map((item): ImportResult<DbBeefyProduct, number> => ({ ...item, success: get(item, "success", true) })),
  );
}
