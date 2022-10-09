import * as Rx from "rxjs";
import { cloneDeep, max } from "lodash";
import { addMissingImportStatus, DbImportStatus, updateImportStatus } from "./import-status";
import { rangeArrayExclude, rangeMerge } from "../../../utils/range";
import { bufferUntilKeyChanged } from "../../../utils/rxjs/utils/buffer-until-key-change";
import { rootLogger } from "../../../utils/logger";
import { PoolClient } from "pg";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { ProductImportQuery } from "../types/product-query";
import { DbProduct } from "./product";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";

const logger = rootLogger.child({ module: "common", component: "block-ranges-import-status" });

export function addMissingBlockRangesImportStatus$<TProduct extends DbProduct>(options: {
  client: PoolClient;
  chain: Chain;
  rpcConfig: RpcConfig;
  getContractAddress: (obj: TProduct) => string;
}): Rx.OperatorFunction<TProduct, { product: TProduct; importStatus: DbImportStatus }> {
  return Rx.pipe(
    addMissingImportStatus<TProduct>({
      client: options.client,
      chain: options.chain,
      rpcConfig: options.rpcConfig,
      getContractAddress: options.getContractAddress,
      getInitialImportData: (_, contractCreationInfo) => ({
        type: "block-ranges",
        data: {
          chainLatestBlockNumber: contractCreationInfo.blockNumber,
          contractCreatedAtBlock: contractCreationInfo.blockNumber,
          lastImportDate: new Date(),
          coveredBlockRanges: [],
          blockRangesToRetry: [],
        },
      }),
    }),
  );
}

export function updateBlockRangesImportStatus$(options: { client: PoolClient; streamConfig: BatchStreamConfig }) {
  return Rx.pipe(
    // merge the statuses ranges together to call updateImportStatus less often
    // but flush often enough so we don't go too long before updating the status
    bufferUntilKeyChanged({
      getKey: (item: ProductImportQuery<DbProduct> & { success: boolean }) => {
        // also make sure we flush every now and then
        const flushTimeKey = Math.floor(Date.now() / options.streamConfig.maxInputWaitMs);
        options.streamConfig.maxInputTake;
        return `${item.product.productId}-${flushTimeKey}`;
      },
      logInfos: { msg: "Merging block ranges import status", data: {} },
      maxBufferSize: options.streamConfig.maxInputTake,
      maxBufferTimeMs: options.streamConfig.maxInputWaitMs,
      pollFrequencyMs: 150,
      pollJitterMs: 50,
    }),

    // update the import status with the new block range
    updateImportStatus({
      client: options.client,
      getProductId: (items) => items[0].product.productId,
      mergeImportStatus: (items, importStatus) => {
        const productId = items[0].product.productId;
        const productKey = items[0].product.productKey;
        if (importStatus.importData.type !== "block-ranges") {
          throw new Error(`Import status is not for block-ranges strategy: ${importStatus.importData.type}`);
        }

        const blockRanges = items.map((item) => item.blockRange);
        const newImportStatus = cloneDeep(importStatus);

        // either way, we covered this range, we need to remember which range we covered
        const mergedRanges = rangeMerge([...newImportStatus.importData.data.coveredBlockRanges, ...blockRanges]);
        newImportStatus.importData.data.coveredBlockRanges = mergedRanges; // only take the first one

        const successRanges = rangeMerge(items.filter((item) => item.success).map((item) => item.blockRange));
        const errorRanges = rangeMerge(items.filter((item) => !item.success).map((item) => item.blockRange));

        // remove success from the ranges we need to retry
        newImportStatus.importData.data.blockRangesToRetry = rangeArrayExclude(newImportStatus.importData.data.blockRangesToRetry, successRanges);
        // add error ranges to the ranges we need to retry
        newImportStatus.importData.data.blockRangesToRetry = rangeMerge([...newImportStatus.importData.data.blockRangesToRetry, ...errorRanges]);

        const rangeResults = items.map((item) => ({ range: item.blockRange, success: item.success }));

        // update the latest block number we know about
        newImportStatus.importData.data.chainLatestBlockNumber = Math.max(
          max(items.map((item) => item.latestBlockNumber)) || 0,
          newImportStatus.importData.data.chainLatestBlockNumber || 0 /* in case it's not set yet */,
        );

        // update the last import date
        newImportStatus.importData.data.lastImportDate = new Date();

        logger.debug({
          msg: "Updating import status",
          data: {
            successRanges: rangeMerge(rangeResults.filter((item) => item.success).map((item) => item.range)),
            errorRanges: rangeMerge(rangeResults.filter((item) => !item.success).map((item) => item.range)),
            product: { productId, productKey },
            importStatus,
            newImportStatus,
          },
        });

        return newImportStatus;
      },
      formatOutput: (items, importStatusUpdated) => items.map((item) => ({ ...item, importStatusUpdated })),
    }),

    // flatten the items
    Rx.mergeAll(),

    // logging
    Rx.tap((item) => {
      if (!item.success) {
        logger.trace({ msg: "Failed to import historical data", data: { productId: item.product.productData, blockRange: item.blockRange } });
      }
    }),
  );
}
