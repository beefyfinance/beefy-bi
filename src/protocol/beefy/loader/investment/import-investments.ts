import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../../types/chain";
import { PoolClient } from "pg";
import { ethers } from "ethers";
import { sample, sortBy } from "lodash";
import { memoryBackpressure$ } from "../../../common/utils/memory-backpressure";
import { RPC_URLS } from "../../../../utils/config";
import { addDebugLogsToProvider, monkeyPatchEthersBatchProvider } from "../../../../utils/ethers";
import { DbBeefyProduct, DbProduct } from "../../../common/loader/product";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../../common/connector/block-query";
import { getRpcLimitations } from "../../../../utils/rpc/rpc-limitations";
import { ProductImportQuery } from "../../../common/types/product-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { createObservableWithNext } from "../../../../utils/rxjs/utils/create-observable-with-next";
import { RpcConfig } from "../../../../types/rpc-config";
import { importProductBlockRange$ } from "./product-block-range";
import { addMissingBlockRangesImportStatus$, updateBlockRangesImportStatus$ } from "../../../common/loader/block-ranges-import-status";
import { rootLogger } from "../../../../utils/logger";
import { rangeArrayExclude, rangeExclude } from "../../../../utils/range";

function createRpcConfig(chain: Chain): RpcConfig {
  const rpcOptions: ethers.utils.ConnectionInfo = {
    url: sample(RPC_URLS[chain]) as string,
    timeout: 120_000,
  };
  const rpcConfig: RpcConfig = {
    chain,
    linearProvider: new ethers.providers.JsonRpcProvider(rpcOptions),
    batchProvider: new ethers.providers.JsonRpcBatchProvider(rpcOptions),
    limitations: getRpcLimitations(chain, rpcOptions.url),
  };

  addDebugLogsToProvider(rpcConfig.linearProvider);
  addDebugLogsToProvider(rpcConfig.batchProvider);
  monkeyPatchEthersBatchProvider(rpcConfig.batchProvider);
  return rpcConfig;
}

export function importChainHistoricalData$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const logger = rootLogger.child({ module: "beefy", component: "import-historical-data" });
  const rpcConfig = createRpcConfig(chain);

  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };
  const {
    observable: productErrors$,
    next: emitErrors,
    complete: completeProductErrors$,
  } = createObservableWithNext<ProductImportQuery<DbProduct>>();

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: DbBeefyProduct) => true),

    addMissingBlockRangesImportStatus$({
      client,
      chain,
      rpcConfig,
      getContractAddress: (product) =>
        product.productData.type === "beefy:vault" ? product.productData.vault.contract_address : product.productData.boost.contract_address,
    }),

    // process first the products we imported the least
    Rx.pipe(
      Rx.toArray(),
      Rx.map((items) => sortBy(items, (item) => item.importStatus.importData.data.lastImportDate)),
      Rx.concatAll(),
    ),

    Rx.pipe(
      // generate the block ranges to import
      addHistoricalBlockQuery$({
        client,
        chain,
        rpcConfig,
        forceCurrentBlockNumber,
        streamConfig,
        getImportStatus: (item) => item.importStatus,
        formatOutput: (item, latestBlockNumber, blockQueries) => ({ ...item, blockQueries, latestBlockNumber }),
      }),

      // convert to stream of product queries
      Rx.concatMap((item) =>
        item.blockQueries.map((blockRange): ProductImportQuery<DbBeefyProduct> => {
          const { blockQueries, ...rest } = item;
          return { ...rest, blockRange, latestBlockNumber: item.latestBlockNumber };
        }),
      ),
    ),

    // some backpressure mechanism
    Rx.pipe(
      memoryBackpressure$({
        logData: { msg: "import-historical-data", data: { chain } },
        sendBurstsOf: streamConfig.maxInputTake,
      }),

      Rx.tap((item) => logger.info({ msg: "processing product", data: { productId: item.product.productId, blockRange: item.blockRange } })),
    ),

    // process the queries
    importProductBlockRange$({ client, chain, streamConfig, rpcConfig, emitErrors: emitErrors }),

    // handle the results
    Rx.pipe(
      // make sure we close the errors observable when we are done
      Rx.finalize(() => setTimeout(completeProductErrors$, 1000)),
      // merge the errors back in, all items here should have been successfully treated
      Rx.mergeWith(productErrors$),
      Rx.map((item) => ({ ...item, success: "success" in item ? item.success : false })),
    ),

    updateBlockRangesImportStatus$({ client, streamConfig }),

    Rx.finalize(() => logger.info({ msg: "Finished importing historical data", data: { chain } })),
  );
}

export function importChainRecentData$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const logger = rootLogger.child({ module: "beefy", component: "import-live-data" });

  const rpcConfig = createRpcConfig(chain);

  const streamConfig: BatchStreamConfig = {
    // since we are doing live data on a small amount of queries (one per vault)
    // we can afford some amount of concurrency
    workConcurrency: 10,
    // But we can not afford to wait before processing the next batch
    maxInputWaitMs: 5_000,
    maxInputTake: 500,
    // and we cannot afford too long of a retry per product
    maxTotalRetryMs: 10_000,
  };
  const { observable: productErrors$, next: emitErrors } = createObservableWithNext<ProductImportQuery<DbProduct>>();

  // remember the last imported block number for each chain so we can reduce the amount of data we fetch
  type ImportState = {
    [key in Chain]: { lastImportedBlockNumber: number | null };
  };
  const importState: ImportState = allChainIds.reduce(
    (agg, chain) => Object.assign(agg, { [chain]: { lastImportedBlockNumber: null } }),
    {} as ImportState,
  );

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: DbBeefyProduct) => true),

    // create an object we can safely add data to
    Rx.map((product) => ({ product })),

    // only live boosts and vaults
    Rx.filter(({ product }) =>
      product.productData.type === "beefy:vault" ? product.productData.vault.eol === false : product.productData.boost.eol === false,
    ),

    // find out the blocks we want to query
    addLatestBlockQuery$({
      chain,
      rpcConfig,
      forceCurrentBlockNumber,
      streamConfig: streamConfig,
      getLastImportedBlock: () => importState[chain].lastImportedBlockNumber ?? null,
      formatOutput: (item, latestBlockNumber, blockRange) => ({ ...item, blockRange, latestBlockNumber }),
    }),

    // process the queries
    importProductBlockRange$({ client, chain, streamConfig, rpcConfig, emitErrors }),

    // merge the errors back in, all items here should have been successfully treated
    Rx.pipe(
      Rx.mergeWith(productErrors$),
      Rx.map((item) => ({ ...item, success: "success" in item ? item.success : false })),

      // logging
      Rx.tap((item) => {
        if (item.success) {
          logger.debug({
            msg: "Imported live data",
            data: { productId: item.product.productData, blockRange: item.blockRange, success: item.success },
          });
        } else {
          logger.error({ msg: "Failed to import live data", data: { productId: item.product.productData, blockRange: item.blockRange } });
        }
      }),
      Rx.finalize(() => logger.info({ msg: "Finished importing live data", data: { chain } })),
    ),
  );
}

/*

export function importChainBalanceSnapshots$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const logger = rootLogger.child({ module: "beefy", component: "import-balance-snapshots" });

  const rpcConfig = createRpcConfig(chain);

  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 30 * 1000,
    maxInputTake: 500,
    // and we can affort longer retries
    maxTotalRetryMs: 30_000,
  };

  const { observable: productErrors$, next: emitErrors } = createObservableWithNext<ProductImportQuery<DbProduct>>();

  return Rx.pipe(
    // add typings to the input item
    Rx.filter((_: DbBeefyProduct) => true),

    addMissingBlockRangesImportStatus$({
      client,
      chain,
      rpcConfig,
      getContractAddress: (product) =>
        product.productData.type === "beefy:vault" ? product.productData.vault.contract_address : product.productData.boost.contract_address,
    }),

    // process first the products we imported the least
    Rx.pipe(
      Rx.toArray(),
      Rx.map((items) => sortBy(items, (item) => item.importStatus.importData.snapsthot.lastImportDate)),
      Rx.concatAll(),
    ),

    // find out which investor+product need snapshoting
    // take into account imported data (we don't want to create snapshots for data ranges in error or not covered)
    // generate the 0 transfer
    Rx.pipe(
      Rx.map((product) => {
        const historical = product.importStatus.importData.historical;
        const snapshot = product.importStatus.importData.snapsthot;

        // get the range of blocks where we have valid historical data for
        let ranges = historical.coveredBlockRanges;
        ranges = rangeArrayExclude(ranges, historical.blockRangesToRetry);

        // remove the ranges where we already have snapshots
        ranges = rangeArrayExclude(ranges, snapshot.);
      }),
    ),

    // some backpressure mechanism
    Rx.pipe(
      memoryBackpressure$({
        logData: { msg: "import-historical-data", data: { chain } },
        sendBurstsOf: streamConfig.maxInputTake,
      }),

      Rx.tap((item) => logger.info({ msg: "processing product", data: { productId: item.product.productId, blockRange: item.blockRange } })),
    ),

    // fetch ppfs for this transfer
    // import the transfer (get balance)
    importTransfers$,

    // handle the results and errors
    Rx.pipe(
      // make sure we close the errors observable when we are done
      Rx.finalize(() => setTimeout(completeProductErrors$, 1000)),
      // merge the errors back in, all items here should have been successfully treated
      Rx.mergeWith(productErrors$),
      Rx.map((item) => ({ ...item, success: "success" in item ? item.success : false })),
    ),

    // update the import status
    updateBlockRangesImportStatus$({ client, streamConfig }),

    Rx.finalize(() => logger.info({ msg: "Finished importing investor snapshots data", data: { chain } })),
  );
}
*/
