import { cloneDeep, keyBy, max } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { RpcConfig } from "../../../types/rpc-config";
import { BATCH_DB_INSERT_SIZE, BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../utils/config";
import { db_query, db_query_one, db_transaction } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { hydrateNumberImportRangesFromDb, ImportRanges, updateImportRanges } from "../utils/import-ranges";
import { ProgrammerError } from "../../../utils/programmer-error";
import { rangeMerge } from "../../../utils/range";
import { bufferUntilKeyChanged } from "../../../utils/rxjs/utils/buffer-until-key-change";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { ImportResult } from "../types/import-query";

const logger = rootLogger.child({ module: "common-loader", component: "import-state" });

interface DbBaseImportState {
  importKey: string;
}

export interface DbProductInvestmentImportState extends DbBaseImportState {
  importData: {
    type: "product:investment";
    productId: number;
    chain: Chain;
    contractCreatedAtBlock: number;
    chainLatestBlockNumber: number;
    ranges: ImportRanges<number>;
  };
}
export interface DbOraclePriceImportState extends DbBaseImportState {
  importData: {
    type: "oracle:price";
    priceFeedId: number;
    ranges: ImportRanges<number>;
  };
}
export interface DbProductShareRateImportState extends DbBaseImportState {
  importData: {
    type: "product:share-rate";
    priceFeedId: number;
    ranges: ImportRanges<number>;
  };
}
export type DbImportState = DbProductInvestmentImportState | DbOraclePriceImportState | DbProductShareRateImportState;

export function isProductInvestmentImportState(o: DbImportState): o is DbProductInvestmentImportState {
  return o.importData.type === "product:investment";
}
export function isOraclePriceImportState(o: DbImportState): o is DbOraclePriceImportState {
  return o.importData.type === "oracle:price";
}
export function isProductShareRateImportState(o: DbImportState): o is DbProductShareRateImportState {
  return o.importData.type === "product:share-rate";
}

export function upsertImportState$<TInput, TRes>(options: {
  client: PoolClient;
  getImportStateData: (obj: TInput) => DbImportState;
  formatOutput: (obj: TInput, importState: DbImportState) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_INSERT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, importStateData: options.getImportStateData(obj) }));

      const results = await db_query<DbImportState>(
        `INSERT INTO import_state (import_key, import_data) VALUES %L
            ON CONFLICT (import_key) 
            -- this may not be the right way to merge our data but it's a start
            DO UPDATE SET import_data = jsonb_merge(import_state.import_data, EXCLUDED.import_data)
            RETURNING import_key as "importKey", import_data as "importData"`,
        [objAndData.map((obj) => [obj.importStateData.importKey, obj.importStateData.importData])],
        options.client,
      );

      const idMap = keyBy(results, "importKey");
      return objAndData.map((obj) => {
        const importState = idMap[obj.importStateData.importKey];
        if (!importState) {
          throw new ProgrammerError({ msg: "Upserted import state not found", data: obj });
        }
        hydrateImportStateRangesFromDb(importState);
        return options.formatOutput(obj.obj, importState);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function fetchImportState$<TObj, TRes>(options: {
  client: PoolClient;
  getImportStateKey: (obj: TObj) => string;
  formatOutput: (obj: TObj, importState: DbImportState | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_SELECT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, importKey: options.getImportStateKey(obj) }));

      const results = await db_query<DbImportState>(
        `SELECT 
            import_key as "importKey",
            import_data as "importData"
          FROM import_state
          WHERE import_key IN (%L)`,
        [objAndData.map((obj) => obj.importKey)],
        options.client,
      );

      const idMap = keyBy(results, "importKey");
      return objAndData.map((obj) => {
        const importState = idMap[obj.importKey] ?? null;

        if (importState) {
          hydrateImportStateRangesFromDb(importState);
        }

        return options.formatOutput(obj.obj, importState);
      });
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function updateImportState$<
  TTarget,
  TObj extends ImportResult<TTarget>,
  TRes extends ImportResult<TTarget>,
  TImport extends DbImportState,
>(options: {
  client: PoolClient;
  streamConfig: BatchStreamConfig;
  getImportStateKey: (obj: TObj) => string;
  formatOutput: (obj: TObj, importState: TImport) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  function mergeImportState(items: TObj[], importState: TImport) {
    const newImportState = cloneDeep(importState);

    // update the import rages
    const coveredRanges = items.map((item) => item.blockRange);
    const successRanges = rangeMerge(items.filter((item) => item.success).map((item) => item.blockRange));
    const errorRanges = rangeMerge(items.filter((item) => !item.success).map((item) => item.blockRange));
    const lastImportDate = new Date();
    const newRanges = updateImportRanges(importState.importData.ranges, { coveredRanges, successRanges, errorRanges, lastImportDate });
    newImportState.importData.ranges = newRanges;

    // update the latest block number we know about
    if (isProductInvestmentImportState(importState)) {
      importState.importData.chainLatestBlockNumber = Math.max(
        max(items.map((item) => item.latestBlockNumber)) || 0,
        importState.importData.chainLatestBlockNumber || 0,
      );
    }

    logger.debug({
      msg: "Updating import state",
      data: { successRanges, errorRanges, importState, newImportState },
    });

    return newImportState;
  }

  return Rx.pipe(
    // merge the product import ranges together to call the database less often
    // but flush often enough so we don't go too long before updating the import ranges
    bufferUntilKeyChanged({
      getKey: options.getImportStateKey,
      logInfos: { msg: "Merging import ranges", data: {} },
      maxBufferSize: options.streamConfig.maxInputTake,
      maxBufferTimeMs: options.streamConfig.maxInputWaitMs,
      pollFrequencyMs: 150,
      pollJitterMs: 50,
    }),

    // update the import state with the new block range

    Rx.mergeMap(async (items) => {
      const importKey = options.getImportStateKey(items[0]);

      // we start a transaction as we need to do a select FOR UPDATE
      const newImportState = await db_transaction(async (client) => {
        const importState = await db_query_one<TImport>(
          `SELECT import_key as "importKey", import_data as "importData"
            FROM import_state
            WHERE import_key = %L
            FOR UPDATE`,
          [importKey],
          client,
        );
        if (!importState) {
          throw new Error(`Import state not found for key ${importKey}`);
        }
        hydrateImportStateRangesFromDb(importState);

        const newImportState = mergeImportState(items, importState);
        logger.trace({ msg: "updateImportState$ (merged)", data: newImportState });
        await db_query(
          ` UPDATE import_state
            SET import_data = %L
            WHERE import_key = %L`,
          [newImportState.importData, importKey],
          client,
        );

        return newImportState;
      });

      return items.map((item) => options.formatOutput(item, newImportState));
    }, options.streamConfig.workConcurrency),

    // flatten the items
    Rx.mergeAll(),

    // logging
    Rx.tap((item) => {
      if (!item.success) {
        logger.trace({ msg: "Failed to import historical data", data: { blockRange: item.blockRange } });
      }
    }),
  );
}

export function addMissingImportState$<TObj, TRes>(options: {
  client: PoolClient;
  rpcConfig: RpcConfig;
  chain: Chain;
  getImportStateKey: (obj: TObj) => string;
  addDefaultImportData$: <TTRes>(
    formatOutput: (obj: TObj, defaultImportData: DbImportState["importData"]) => TTRes,
  ) => Rx.OperatorFunction<TObj, TTRes>;
  formatOutput: (obj: TObj, importState: DbImportState) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const addDefaultImportState$ = Rx.pipe(
    // flatten the input items
    Rx.map(({ obj }: { obj: TObj }) => obj),

    // get the import state from the user
    options.addDefaultImportData$((obj, defaultImportData) => ({ obj, defaultImportData })),

    // create the import state in the database
    upsertImportState$({
      client: options.client,
      getImportStateData: (item) =>
        ({
          importKey: options.getImportStateKey(item.obj),
          importData: item.defaultImportData,
        } as DbImportState),
      formatOutput: (item, importState) => ({ obj: item.obj, importState }),
    }),
  );

  return Rx.pipe(
    // find the current import state for these objects (if already created)
    fetchImportState$({
      client: options.client,
      getImportStateKey: options.getImportStateKey,
      formatOutput: (obj, importState) => ({ obj, importState }),
    }),

    // extract those without an import state
    Rx.groupBy((item) => (item.importState !== null ? "has-import-state" : "missing-import-state")),
    Rx.map((importStateGrps$) => {
      // passthrough if we already have a import state
      if (importStateGrps$.key === "has-import-state") {
        return importStateGrps$ as Rx.Observable<{ obj: TObj; importState: DbImportState }>;
      }

      // then for those whe can't find an import state
      return importStateGrps$.pipe(
        Rx.tap((item) => logger.debug({ msg: "Missing import state", data: item })),

        addDefaultImportState$,
      );
    }),
    // now all objects have a import state (and a contract creation block)
    Rx.mergeAll(),

    Rx.map((item) => options.formatOutput(item.obj, item.importState)),
  );
}

function hydrateImportStateRangesFromDb(importState: DbImportState) {
  // hydrate dates properly
  hydrateNumberImportRangesFromDb(importState.importData.ranges);
}
