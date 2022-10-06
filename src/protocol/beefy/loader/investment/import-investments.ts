import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../../types/chain";
import { PoolClient } from "pg";
import { ethers } from "ethers";
import { sample } from "lodash";
import { BACKPRESSURE_CHECK_INTERVAL_MS, BACKPRESSURE_MEMORY_THRESHOLD_MB, RPC_URLS } from "../../../../utils/config";
import { addDebugLogsToProvider } from "../../../../utils/ethers";
import { DbBeefyProduct, DbProduct } from "../../../common/loader/product";
import { addHistoricalBlockQuery$, addLatestBlockQuery$ } from "../../../common/connector/block-query";
import { getRpcLimitations } from "../../../../utils/rpc/rpc-limitations";
import { ProductImportQuery } from "../../../common/types/product-query";
import { BatchStreamConfig } from "../../../common/utils/batch-rpc-calls";
import { createObservableWithNext } from "../../../../utils/rxjs/utils/create-observable-with-next";
import { bufferUntilBelowMachineThresholds } from "../../../../utils/rxjs/utils/buffer-until-below-machine-threshold";
import { RpcConfig } from "../../../../types/rpc-config";
import { importProductBlockRange$ } from "./product-block-range";
import { addMissingBlockRangesImportStatus$, updateBeefyImportStatus$ } from "../../../common/loader/block-ranges-import-status";
import { rootLogger } from "../../../../utils/logger";

export function importChainHistoricalData$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const logger = rootLogger.child({ module: "beefy", component: "import-historical-data" });
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

  const streamConfig: BatchStreamConfig = {
    // since we are doing many historical queries at once, we cannot afford to do many at once
    workConcurrency: 1,
    // But we can afford to wait a bit longer before processing the next batch to be more efficient
    maxInputWaitMs: 5 * 60 * 1000,
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

    Rx.pipe(
      // generate the block ranges to import
      addHistoricalBlockQuery$({
        client,
        chain,
        rpcConfig,
        forceCurrentBlockNumber,
        streamConfig,
        getImportStatus: (item) => item.importStatus,
        formatOutput: (item, blockQueries) => ({ ...item, blockQueries }),
      }),

      // convert to stream of product queries
      Rx.concatMap((item) =>
        item.blockQueries.map((blockRange): ProductImportQuery<DbBeefyProduct> => {
          const { blockQueries, ...rest } = item;
          return { ...rest, blockRange };
        }),
      ),
    ),

    // some backpressure mechanism
    Rx.pipe(
      bufferUntilBelowMachineThresholds({
        sendInitialBurstOf: streamConfig.maxInputTake,
        checkIntervalMs: BACKPRESSURE_CHECK_INTERVAL_MS,
        checkIntervalJitterMs: 2000,
        maxMemoryThresholdMb: BACKPRESSURE_MEMORY_THRESHOLD_MB,
        sendByBurstOf: streamConfig.maxInputTake,
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

    updateBeefyImportStatus$({ client, streamConfig }),

    Rx.finalize(() => logger.info({ msg: "Finished importing historical data", data: { chain } })),
  );
}

export function importChainRecentData$(client: PoolClient, chain: Chain, forceCurrentBlockNumber: number | null) {
  const logger = rootLogger.child({ module: "beefy", component: "import-live-data" });
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
      formatOutput: (item, blockRange) => ({ ...item, blockRange }),
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
