import { cloneDeep, keyBy, max } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { BATCH_DB_INSERT_SIZE, BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query, db_query_one, db_transaction } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { hydrateNumberImportRangesFromDb, ImportRanges, updateImportRanges } from "../utils/import-ranges";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { ProgrammerError } from "../../../utils/programmer-error";
import { fetchContractCreationBlock$ } from "../connector/contract-creation";
import { DbProduct } from "./product";
import { rangeMerge } from "../../../utils/range";
import { bufferUntilKeyChanged } from "../../../utils/rxjs/utils/buffer-until-key-change";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { ProductImportResult } from "../types/product-query";

const logger = rootLogger.child({ module: "common-loader", component: "product-import" });

export interface DbProductImport {
  productId: number;
  importData: {
    contractCreatedAtBlock: number;
    chainLatestBlockNumber: number;
    ranges: ImportRanges<number>;
  };
}

export function upsertProductImport$<TInput, TRes>(options: {
  client: PoolClient;
  getProductImportData: (obj: TInput) => DbProductImport;
  formatOutput: (obj: TInput, productImport: DbProductImport) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, productImportData: options.getProductImportData(obj) }));

      const results = await db_query<DbProductImport>(
        `INSERT INTO product_import (product_id, import_data) VALUES %L
            ON CONFLICT (product_id) 
            -- this may not be the right way to merge our data but it's a start
            DO UPDATE SET import_data = jsonb_merge(product_import.import_data, EXCLUDED.import_data)
            RETURNING product_id as "productId", import_data as "importData"`,
        [objAndData.map((obj) => [obj.productImportData.productId, obj.productImportData.importData])],
        options.client,
      );

      const idMap = keyBy(results, "productId");
      return objAndData.map((obj) => {
        const productImport = idMap[obj.productImportData.productId];
        if (!productImport) {
          throw new ProgrammerError({ msg: "Upserted product import not found", data: obj });
        }
        hydrateProductImportRangesFromDb(productImport);
        return options.formatOutput(obj.obj, productImport);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function fetchProductImport$<TObj, TRes>(options: {
  client: PoolClient;
  getProductId: (obj: TObj) => number;
  formatOutput: (obj: TObj, productImport: DbProductImport | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_SELECT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, productId: options.getProductId(obj) }));

      const results = await db_query<DbProductImport>(
        `SELECT 
            product_id as "productId",
            import_data as "importData"
          FROM product_import
          WHERE product_id IN (%L)`,
        [objAndData.map((obj) => obj.productId)],
        options.client,
      );

      const idMap = keyBy(results, "productId");
      return objAndData.map((obj) => {
        const productImport = idMap[obj.productId] ?? null;

        if (productImport) {
          hydrateProductImportRangesFromDb(productImport);
        }

        return options.formatOutput(obj.obj, productImport);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function updateProductImport$<TProduct extends DbProduct>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
}): Rx.OperatorFunction<ProductImportResult<TProduct>, ProductImportResult<TProduct>> {
  function mergeProductImport(items: ProductImportResult<TProduct>[], productImport: DbProductImport) {
    const productId = items[0].product.productId;
    const productKey = items[0].product.productKey;

    const newProductImport = cloneDeep(productImport);

    // update the import rages
    const coveredRanges = items.map((item) => item.blockRange);
    const successRanges = rangeMerge(items.filter((item) => item.success).map((item) => item.blockRange));
    const errorRanges = rangeMerge(items.filter((item) => !item.success).map((item) => item.blockRange));
    const lastImportDate = new Date();
    const newRanges = updateImportRanges(productImport.importData.ranges, { coveredRanges, successRanges, errorRanges, lastImportDate });
    newProductImport.importData.ranges = newRanges;

    // update the latest block number we know about
    productImport.importData.chainLatestBlockNumber = Math.max(
      max(items.map((item) => item.latestBlockNumber)) || 0,
      newProductImport.importData.chainLatestBlockNumber || 0 /* in case it's not set yet */,
    );

    logger.debug({
      msg: "Updating product import",
      data: { successRanges, errorRanges, product: { productId, productKey }, productImport, newProductImport },
    });

    return newProductImport;
  }

  return Rx.pipe(
    // merge the product import ranges together to call the database less often
    // but flush often enough so we don't go too long before updating the import ranges
    bufferUntilKeyChanged({
      getKey: (item) => {
        // also make sure we flush every now and then
        const flushTimeKey = Math.floor(Date.now() / options.streamConfig.maxInputWaitMs);
        options.streamConfig.maxInputTake;
        return `${item.product.productId}-${flushTimeKey}`;
      },
      logInfos: { msg: "Merging product import ranges", data: {} },
      maxBufferSize: options.streamConfig.maxInputTake,
      maxBufferTimeMs: options.streamConfig.maxInputWaitMs,
      pollFrequencyMs: 150,
      pollJitterMs: 50,
    }),

    // update the product import with the new block range

    Rx.mergeMap(async (items) => {
      const productId = items[0].product.productId;

      // we start a transaction as we need to do a select FOR UPDATE
      const newImportRanges = await db_transaction(async (client) => {
        const importRanges = await db_query_one<DbProductImport>(
          `SELECT product_id as "productId", import_data as "importData"
            FROM product_import
            WHERE product_id = %L
            FOR UPDATE`,
          [productId],
          client,
        );
        if (!importRanges) {
          throw new Error(`product Import range not found for product ${productId}`);
        }
        hydrateProductImportRangesFromDb(importRanges);

        const newImportRanges = mergeProductImport(items, importRanges);
        logger.trace({ msg: "updateProductImport$ (merged)", data: newImportRanges });
        await db_query(
          ` UPDATE product_import
            SET import_data = %L
            WHERE product_id = %L`,
          [newImportRanges.importData, productId],
          client,
        );

        return newImportRanges;
      });

      return items.map((item) => ({ ...item, newImportRanges }));
    }, 1 /* concurrency */),

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

export function addMissingProductImport$<TProduct extends DbProduct, TRes>(options: {
  client: PoolClient;
  rpcConfig: RpcConfig;
  chain: Chain;
  getContractAddress: (product: TProduct) => string;
  formatOutput: (product: TProduct, productImport: DbProductImport) => TRes;
}): Rx.OperatorFunction<TProduct, TRes> {
  return Rx.pipe(
    // find the product import for these objects
    fetchProductImport$({
      client: options.client,
      getProductId: (product) => product.productId,
      formatOutput: (product, productImport) => ({ product, productImport }),
    }),

    // extract those without a product import
    Rx.groupBy((item) => (item.productImport !== null ? "has-product-import" : "missing-product-import")),
    Rx.map((productImportGroups$) => {
      // passthrough if we already have a product import
      if (productImportGroups$.key === "has-product-import") {
        return productImportGroups$ as Rx.Observable<{ product: TProduct; productImport: DbProductImport }>;
      }

      // then for those whe can't find a product import
      return productImportGroups$.pipe(
        Rx.tap((item) => logger.debug({ msg: "Missing product import for product", data: item })),

        // find the contract creation block
        fetchContractCreationBlock$({
          rpcConfig: options.rpcConfig,
          getCallParams: (item) => ({
            chain: options.chain,
            contractAddress: options.getContractAddress(item.product),
          }),
          formatOutput: (item, contractCreationInfo) => ({ ...item, contractCreationInfo }),
        }),

        // drop those without a creation info
        excludeNullFields$("contractCreationInfo"),

        // create the product import
        upsertProductImport$({
          client: options.client,
          getProductImportData: (item) => ({
            productId: item.product.productId,
            importData: {
              chainLatestBlockNumber: item.contractCreationInfo.blockNumber,
              contractCreatedAtBlock: item.contractCreationInfo.blockNumber,
              ranges: {
                lastImportDate: new Date(),
                coveredRanges: [],
                toRetry: [],
              },
            },
          }),
          formatOutput: (item, productImport) => ({ ...item, productImport }),
        }),
      );
    }),
    // now all objects have a product import (and a contract creation block)
    Rx.mergeAll(),

    Rx.map((item) => options.formatOutput(item.product, item.productImport)),
  );
}

function hydrateProductImportRangesFromDb(productImport: DbProductImport) {
  // hydrate dates properly
  hydrateNumberImportRangesFromDb(productImport.importData.ranges);
}
