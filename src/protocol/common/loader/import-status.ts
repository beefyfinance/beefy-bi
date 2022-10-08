import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { BATCH_DB_INSERT_SIZE, BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query, db_query_one, db_transaction } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { Range } from "../../../utils/range";
import { excludeNullFields$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ContractCreationInfos, fetchContractCreationBlock$ } from "../connector/contract-creation";
import { DbProduct } from "./product";

const logger = rootLogger.child({ module: "price-feed", component: "loader" });

interface BlockRangesImportStatus {
  lastImportDate: Date;

  chainLatestBlockNumber: number;

  contractCreatedAtBlock: number;
  // already imported once range
  coveredBlockRange: Range;
  // ranges where an error occured
  blockRangesToRetry: Range[];
}

export interface DbImportStatus {
  productId: number;
  importData: {
    type: "block-ranges";
    data: BlockRangesImportStatus;
  };
}

export function upsertImportStatus$<TInput, TRes>(options: {
  client: PoolClient;
  getImportStatusData: (obj: TInput) => DbImportStatus;
  formatOutput: (obj: TInput, importStatus: DbImportStatus) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, importStatusData: options.getImportStatusData(obj) }));

      const results = await db_query<DbImportStatus>(
        `INSERT INTO import_status (product_id, import_data) VALUES %L
            ON CONFLICT (product_id) 
            -- this may not be the right way to merge our data but it's a start
            DO UPDATE SET import_data = jsonb_merge(import_status.import_data, EXCLUDED.import_data)
            RETURNING product_id as "productId", import_data as "importData"`,
        [objAndData.map((obj) => [obj.importStatusData.productId, obj.importStatusData.importData])],
        options.client,
      );

      const idMap = keyBy(results, "productId");
      return objAndData.map((obj) => {
        const importStatus = idMap[obj.importStatusData.productId];
        if (!importStatus) {
          throw new ProgrammerError({ msg: "Upserted import status not found", data: obj });
        }
        importStatus.importData.data.lastImportDate = new Date(importStatus.importData.data.lastImportDate || 0);
        return options.formatOutput(obj.obj, importStatus);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function fetchImportStatus$<TObj, TRes>(options: {
  client: PoolClient;
  getProductId: (obj: TObj) => number;
  formatOutput: (obj: TObj, importStatus: DbImportStatus | null) => TRes;
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

      const results = await db_query<DbImportStatus>(
        `SELECT 
            product_id as "productId",
            import_data as "importData"
          FROM import_status
          WHERE product_id IN (%L)`,
        [objAndData.map((obj) => obj.productId)],
        options.client,
      );

      const idMap = keyBy(results, "productId");
      return objAndData.map((obj) => {
        const importStatus = idMap[obj.productId] ?? null;
        if (importStatus) {
          importStatus.importData.data.lastImportDate = new Date(importStatus.importData.data.lastImportDate || 0);
        }
        return options.formatOutput(obj.obj, importStatus);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function updateImportStatus<TObj, TRes>(options: {
  client: PoolClient;
  getProductId: (obj: TObj) => number;
  mergeImportStatus: (obj: TObj, importStatus: DbImportStatus) => DbImportStatus;
  formatOutput: (obj: TObj, feed: DbImportStatus) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.mergeMap(async (obj: TObj) => {
      const productId = options.getProductId(obj);

      const newImportStatus = await db_transaction(async (client) => {
        const importStatus = await db_query_one<DbImportStatus>(
          `SELECT product_id as "productId", import_data as "importData"
            FROM import_status
            WHERE product_id = %L
            FOR UPDATE`,
          [productId],
          client,
        );
        if (!importStatus) {
          throw new Error(`Import status not found for product ${productId}`);
        }

        const newImportStatus = options.mergeImportStatus(obj, importStatus);
        logger.trace({ msg: "updateImportStatus (merged)", data: newImportStatus });
        await db_query(
          ` UPDATE import_status
            SET import_data = %L
            WHERE product_id = %L`,
          [newImportStatus.importData, productId],
          client,
        );

        return newImportStatus;
      });

      return options.formatOutput(obj, newImportStatus);
    }, 1 /* concurrency */),
  );
}

export function addMissingImportStatus<TProduct extends DbProduct>(options: {
  client: PoolClient;
  rpcConfig: RpcConfig;
  chain: Chain;
  getContractAddress: (product: TProduct) => string;
  getInitialImportData: (product: TProduct, creationInfos: ContractCreationInfos) => DbImportStatus["importData"];
}): Rx.OperatorFunction<TProduct, { product: TProduct; importStatus: DbImportStatus }> {
  return Rx.pipe(
    // find the import status for these objects
    fetchImportStatus$({
      client: options.client,
      getProductId: (product) => product.productId,
      formatOutput: (product, importStatus) => ({ product, importStatus }),
    }),

    // extract those without an import status
    Rx.groupBy((item) => (item.importStatus !== null ? "has-import-status" : "missing-import-status")),
    Rx.map((importStatusGroup$) => {
      // passthrough if we already have an import status
      if (importStatusGroup$.key === "has-import-status") {
        return importStatusGroup$ as Rx.Observable<{ product: TProduct; importStatus: DbImportStatus }>;
      }

      // then for those whe can't find an import status
      return importStatusGroup$.pipe(
        Rx.tap((item) => logger.debug({ msg: "Missing import status for product", data: item })),

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

        // create the import status
        upsertImportStatus$({
          client: options.client,
          getImportStatusData: (item) => ({
            productId: item.product.productId,
            importData: options.getInitialImportData(item.product, item.contractCreationInfo),
          }),
          formatOutput: (item, importStatus) => ({ ...item, importStatus }),
        }),
      );
    }),
    // now all objects have an import status (and a contract creation block)
    Rx.mergeAll(),
  );
}
