import { backOff } from "exponential-backoff";
import { cloneDeep, groupBy, keyBy, uniq } from "lodash";
import * as Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { ConnectionTimeoutError } from "../../../utils/async";
import { DbClient, db_query, db_transaction } from "../../../utils/db";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import {
  isDateRange,
  isNumberRange,
  Range,
  rangeArrayExclude,
  rangeArrayOverlap,
  rangeExcludeMany,
  rangeManyOverlap,
  rangeMerge,
  rangeOverlap,
  rangeSortedArrayExclude,
  rangeValueMax,
  SupportedRangeTypes,
} from "../../../utils/range";
import { ErrorEmitter, ImportCtx } from "../types/import-context";
import { ImportRangeResult } from "../types/import-query";
import { BatchStreamConfig } from "../utils/batch-rpc-calls";
import { hydrateDateImportRangesFromDb, hydrateNumberImportRangesFromDb, ImportRanges, updateImportRanges } from "../utils/import-ranges";

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
    contractCreationDate: Date;
    chainLatestBlockNumber: number;
    ranges: ImportRanges<number>;
  };
}
export interface DbOraclePriceImportState extends DbBaseImportState {
  importData: {
    type: "oracle:price";
    priceFeedId: number;
    firstDate: Date;
    ranges: ImportRanges<Date>;
  };
}
export interface DbProductShareRateImportState extends DbBaseImportState {
  importData: {
    type: "product:share-rate";
    priceFeedId: number;
    productId: number;
    chain: Chain;
    contractCreatedAtBlock: number;
    contractCreationDate: Date;
    chainLatestBlockNumber: number;
    ranges: ImportRanges<number>;
  };
}
export interface DbPendingRewardsImportState extends DbBaseImportState {
  importData: {
    type: "rewards:snapshots";
    productId: number;
    investorId: number;
    chain: Chain;
    contractCreatedAtBlock: number;
    contractCreationDate: Date;
    chainLatestBlockNumber: number;
    ranges: ImportRanges<number>;
  };
}

export type DbBlockNumberRangeImportState = DbProductInvestmentImportState | DbProductShareRateImportState | DbPendingRewardsImportState;
export type DbDateRangeImportState = DbOraclePriceImportState;
export type DbImportState = DbBlockNumberRangeImportState | DbDateRangeImportState;

export function isProductInvestmentImportState(o: DbImportState): o is DbProductInvestmentImportState {
  return o.importData.type === "product:investment";
}
export function isOraclePriceImportState(o: DbImportState): o is DbOraclePriceImportState {
  return o.importData.type === "oracle:price";
}
export function isProductShareRateImportState(o: DbImportState): o is DbProductShareRateImportState {
  return o.importData.type === "product:share-rate";
}
export function isPendingRewardsImportState(o: DbImportState): o is DbPendingRewardsImportState {
  return o.importData.type === "rewards:snapshots";
}

function upsertImportState$<TInput, TRes>(options: {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  getImportStateData: (obj: TInput) => DbImportState;
  formatOutput: (obj: TInput, importState: DbImportState) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    Rx.bufferTime(options.streamConfig.dbMaxInputWaitMs, undefined, options.streamConfig.dbMaxInputTake),
    Rx.filter((items) => items.length > 0),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
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
          throw new ProgrammerError({ msg: "import state not found after upsert", data: obj });
        }
        hydrateImportStateRangesFromDb(importState);
        return options.formatOutput(obj.obj, importState);
      });
    }, options.streamConfig.workConcurrency),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function fetchImportState$<TObj, TRes, TImport extends DbImportState>(options: {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  getImportStateKey: (obj: TObj) => string;
  formatOutput: (obj: TObj, importState: TImport | null) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(options.streamConfig.dbMaxInputWaitMs, undefined, options.streamConfig.dbMaxInputTake),
    Rx.filter((items) => items.length > 0),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      const objAndData = objs.map((obj) => ({ obj, importKey: options.getImportStateKey(obj) }));

      const results = await db_query<TImport>(
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
        if (importState !== null) {
          hydrateImportStateRangesFromDb(importState);
        }

        return options.formatOutput(obj.obj, importState);
      });
    }, options.streamConfig.workConcurrency),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}

export function updateImportState$<
  TObj,
  TErr extends ErrorEmitter<TObj>,
  TRes,
  TImport extends DbImportState,
  TRange extends SupportedRangeTypes,
>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getRange: (obj: TObj) => Range<TRange>;
  isSuccess: (obj: TObj) => boolean;
  getImportStateKey: (obj: TObj) => string;
  formatOutput: (obj: TObj, importState: TImport) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  function mergeImportState(items: TObj[], importState: TImport) {
    const range = options.getRange(items[0]);
    if (
      (isProductInvestmentImportState(importState) && !isNumberRange(range)) ||
      (isOraclePriceImportState(importState) && !isDateRange(range)) ||
      (isProductShareRateImportState(importState) && !isNumberRange(range)) ||
      (isPendingRewardsImportState(importState) && !isNumberRange(range))
    ) {
      throw new ProgrammerError({
        msg: "Import state is for product investment but item is not",
        data: { importState, item: items[0] },
      });
    }

    const newImportState = cloneDeep(importState);

    // update the import rages
    let coveredRanges = items.map((item) => options.getRange(item));
    // covered ranges should not be overlapping
    const hasCoveredRangeOverlap = rangeArrayOverlap(coveredRanges);
    if (hasCoveredRangeOverlap) {
      // this should never happen, seeing this in the logs means there is something duplicating input queries
      logger.error({ msg: "covered ranges should not be overlapping", data: { importState, coveredRanges, items } });
      coveredRanges = rangeMerge(coveredRanges);
    }

    let successRanges = rangeMerge(items.filter((item) => options.isSuccess(item)).map((item) => options.getRange(item)));
    const errorRanges = rangeMerge(items.filter((item) => !options.isSuccess(item)).map((item) => options.getRange(item)));

    // success and error ranges should be exclusive
    const hasOverlap = rangeManyOverlap(successRanges, errorRanges);
    if (hasOverlap) {
      logger.error({ msg: "Import state ranges are not exclusive", data: { importState, successRanges, errorRanges } });
      // todo: this should not happen, but it does
      // exclude errors from success ranges
      successRanges = rangeSortedArrayExclude(successRanges, errorRanges);
    }

    const lastImportDate = new Date();
    const newRanges = updateImportRanges<TRange>(newImportState.importData.ranges as ImportRanges<TRange>, {
      coveredRanges,
      successRanges,
      errorRanges,
      lastImportDate,
    });
    (newImportState.importData.ranges as ImportRanges<TRange>) = newRanges;

    // update the latest block number we know about
    if (
      isProductInvestmentImportState(newImportState) ||
      isProductShareRateImportState(newImportState) ||
      isPendingRewardsImportState(newImportState)
    ) {
      newImportState.importData.chainLatestBlockNumber =
        rangeValueMax(
          (items as ImportRangeResult<TObj, number>[]).map((item) => item.latest).concat([newImportState.importData.chainLatestBlockNumber]),
        ) || 0;
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
    Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, options.ctx.streamConfig.maxInputTake),
    Rx.filter((items) => items.length > 0),

    // update multiple import states at once
    Rx.concatMap(async (items) => {
      const logInfos: LogInfos = {
        msg: "import-state update transaction",
        data: { chain: options.ctx.chain, importKeys: uniq(items.map((item) => options.getImportStateKey(item))) },
      };
      // we start a transaction as we need to do a select FOR UPDATE
      const work = () =>
        db_transaction(
          async (client) => {
            const dbImportStates = await db_query<TImport>(
              `SELECT import_key as "importKey", import_data as "importData"
            FROM import_state
            WHERE import_key in (%L)
            ORDER BY import_key -- Remove the possibility of deadlocks https://stackoverflow.com/a/51098442/2523414
            FOR UPDATE`,
              [uniq(items.map((item) => options.getImportStateKey(item)))],
              client,
            );

            const dbImportStateMap = keyBy(
              dbImportStates.map((i) => {
                hydrateImportStateRangesFromDb(i);
                return i;
              }),
              "importKey",
            );
            const inputItemsByImportStateKey = groupBy(items, (item) => options.getImportStateKey(item));
            const newImportStates = Object.entries(inputItemsByImportStateKey).map(([importKey, sameKeyInputItems]) => {
              const dbImportState = dbImportStateMap[importKey];
              if (!dbImportState) {
                throw new ProgrammerError({ msg: "Import state not found", data: { importKey, items } });
              }
              return mergeImportState(sameKeyInputItems, dbImportState);
            });

            logger.trace({ msg: "updateImportState$ (merged)", data: newImportStates });
            await Promise.all(
              newImportStates.map((data) =>
                db_query(`UPDATE import_state SET import_data = %L WHERE import_key = %L`, [data.importData, data.importKey], client),
              ),
            );

            logger.trace({ msg: "updateImportState$ (merged) done", data: newImportStates });

            return newImportStates;
          },
          {
            connectTimeoutMs: 5000,
            queryTimeoutMs: 2000 /* this should be a very quick operation */,
            appName: "beefy:import_state:update_transaction",
            logInfos,
          },
        );

      try {
        logger.debug(mergeLogsInfos({ msg: "Updating import state" }, logInfos));
        const newImportStates = await backOff(() => work(), {
          delayFirstAttempt: false,
          startingDelay: 500,
          timeMultiple: 5,
          maxDelay: 1_000,
          numOfAttempts: 10,
          jitter: "full",
          retry: (error) => {
            if (error instanceof ConnectionTimeoutError) {
              logger.debug(mergeLogsInfos({ msg: "Connection timeout error, will retry", data: { error } }, logInfos));
              return true;
            }
            return false;
          },
        });

        const resultMap = keyBy(newImportStates, "importKey");

        return items.map((item) => {
          const importKey = options.getImportStateKey(item);
          const importState = resultMap[importKey] ?? null;
          if (!importState) {
            logger.error(mergeLogsInfos({ msg: "Import state not found", data: { importKey, items } }, logInfos));
            throw new ProgrammerError({ msg: "Import state not found", data: { importKey, items } });
          }
          return options.formatOutput(item, importState);
        });
      } catch (error) {
        if (error instanceof ConnectionTimeoutError) {
          const report = {
            error,
            infos: mergeLogsInfos(
              {
                msg: "Connection timeout error, will not retry, import state not updated",
                data: {},
              },
              logInfos,
            ),
          };
          logger.debug(report.infos);
          logger.debug(report.error);
          for (const item of items) {
            report.infos.data = { ...report.infos.data, item };
            options.emitError(item, report);
          }
          return [];
        }
        throw error;
      }
    }),

    // flatten the items
    Rx.concatAll(),
  );
}

export function addMissingImportState$<TInput, TRes, TImport extends DbImportState>(options: {
  client: DbClient;
  streamConfig: BatchStreamConfig;
  getImportStateKey: (obj: TInput) => string;
  createDefaultImportState$: Rx.OperatorFunction<TInput, { obj: TInput; importData: TImport["importData"] }>;
  formatOutput: (obj: TInput, importState: TImport) => TRes;
}): Rx.OperatorFunction<TInput, TRes> {
  return Rx.pipe(
    // find the current import state for these objects (if already created)
    fetchImportState$({
      client: options.client,
      streamConfig: options.streamConfig,
      getImportStateKey: options.getImportStateKey,
      formatOutput: (obj, importState) => ({ obj, importState }),
    }),

    Rx.connect((items$) =>
      Rx.merge(
        items$.pipe(
          Rx.filter(({ importState }) => importState !== null),
          Rx.tap((item) => logger.trace({ msg: "import state present", data: { importKey: options.getImportStateKey(item.obj) } })),
        ),
        items$.pipe(
          Rx.filter(({ importState }) => importState === null),
          Rx.tap((item) => logger.debug({ msg: "Missing import state", data: { importKey: options.getImportStateKey(item.obj) } })),
          Rx.map(({ obj }) => obj),
          // get the import state from the user
          options.createDefaultImportState$,

          // create the import state in the database
          upsertImportState$({
            client: options.client,
            streamConfig: options.streamConfig,
            getImportStateData: (item) =>
              ({
                importKey: options.getImportStateKey(item.obj),
                importData: item.importData,
              } as TImport),
            formatOutput: (item, importState) => ({ obj: item.obj, importState }),
          }),
        ),
      ),
    ),

    // fix ts typings
    Rx.filter((item): item is { obj: TInput; importState: TImport } => true),

    Rx.map((item) => options.formatOutput(item.obj, item.importState)),
  );
}

function hydrateImportStateRangesFromDb(importState: DbImportState) {
  const type = importState.importData.type;
  // hydrate dates properly
  if (isProductInvestmentImportState(importState)) {
    importState.importData.contractCreationDate = new Date(importState.importData.contractCreationDate);
    hydrateNumberImportRangesFromDb(importState.importData.ranges);
  } else if (isProductShareRateImportState(importState)) {
    importState.importData.contractCreationDate = new Date(importState.importData.contractCreationDate);
    hydrateNumberImportRangesFromDb(importState.importData.ranges);
  } else if (isOraclePriceImportState(importState)) {
    importState.importData.firstDate = new Date(importState.importData.firstDate);
    hydrateDateImportRangesFromDb(importState.importData.ranges);
  } else if (isPendingRewardsImportState(importState)) {
    importState.importData.contractCreationDate = new Date(importState.importData.contractCreationDate);
    hydrateNumberImportRangesFromDb(importState.importData.ranges);
  } else {
    throw new ProgrammerError(`Unknown import state type ${type}`);
  }
}
