import { keyBy, uniqBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { db_query, db_query_one } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { Range } from "../../../utils/range";
import { batchQueryGroup$ } from "../../../utils/rxjs/utils/batch-query-group";

const logger = rootLogger.child({ module: "price-feed", component: "loader" });

interface BeefyImportStatus {
  contractCreatedAtBlock: number;
  // already imported once range
  coveredBlockRange: Range;
  // ranges where an error occured
  blockRangesToRetry: Range[];
}

export interface DbImportStatus {
  productId: number;
  importData: {
    type: "beefy";
    data: BeefyImportStatus;
  };
}

export function upsertImportStatus$<TInput, TRes>(options: {
  client: PoolClient;
  getImportStatusData: (obj: TInput) => DbImportStatus;
  formatOutput: (obj: TInput, importStatus: DbImportStatus) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    // insert every 1s or 500 items
    Rx.bufferTime(1000, undefined, 500),

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

      // ensure results are in the same order as the params
      const idMap = keyBy(results, "productId");
      return objAndData.map((obj) => options.formatOutput(obj.obj, idMap[obj.importStatusData.productId]));
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
  return batchQueryGroup$({
    bufferCount: 500,
    toQueryObj: (obj: TObj[]) => options.getProductId(obj[0]),
    getBatchKey: (obj: TObj) => options.getProductId(obj),
    processBatch: async (ids: number[]) => {
      const results = await db_query<DbImportStatus>(
        `SELECT 
            product_id as "productId",
            import_data as "importData"
          FROM import_status
          WHERE product_id IN (%L)`,
        [ids],
        options.client,
      );
      // ensure results are in the same order as the params
      const idMap = keyBy(results, (r) => r.productId);
      return ids.map((id) => idMap[id] ?? null);
    },
    formatOutput: options.formatOutput,
  });
}

export function updateImportStatus<TObj, TRes>(options: {
  client: PoolClient;
  getProductId: (obj: TObj) => number;
  mergeImportStatus: (obj: TObj, importStatus: DbImportStatus) => DbImportStatus;
  formatOutput: (obj: TObj, feed: DbImportStatus) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.concatMap(async (obj: TObj) => {
      const productId = options.getProductId(obj);

      try {
        await db_query("begin", [], options.client);

        const importStatus = await db_query_one<DbImportStatus>(
          `SELECT product_id as "productId", import_data as "importData"
            FROM import_status
            WHERE product_id = %L
            FOR UPDATE`,
          [productId],
          options.client,
        );
        if (!importStatus) {
          throw new Error(`Import status not found for product ${productId}`);
        }

        const newImportStatus = options.mergeImportStatus(obj, importStatus);
        await db_query(
          ` UPDATE import_status
            SET import_data = %L
            WHERE product_id = %L`,
          [newImportStatus.importData, productId],
          options.client,
        );

        return options.formatOutput(obj, newImportStatus);
      } finally {
        await db_query("rollback", [], options.client);
      }
    }),
  );
}
