import axios from "axios";
import { isString } from "lodash";
import * as Rx from "rxjs";
import { samplingPeriodMs } from "../../../types/sampling";
import { getChainNetworkId } from "../../../utils/addressbook";
import { ETHERSCAN_API_KEY, EXPLORER_URLS } from "../../../utils/config";
import { db_query } from "../../../utils/db";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { forkOnNullableField$ } from "../../../utils/rxjs/utils/exclude-null-field";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { ErrorEmitter, ImportCtx, Throwable } from "../types/import-context";
import { dbBatchCall$ } from "../utils/db-batch";

const logger = rootLogger.child({ module: "common", component: "block-datetime" });

export function fetchBlockFromDatetime$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends Date>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getBlockDate: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const chain = options.ctx.chain;

  const explorerConfig = EXPLORER_URLS[chain];
  if (explorerConfig.type !== "etherscan" && explorerConfig.type !== "etherscan-v2") {
    throw new ProgrammerError(`etherscan config is missing`);
  }

  return Rx.pipe(
    // add TS typings
    Rx.tap((_: TObj) => {}),

    // DB-first lookup: if we already have a block within ±120s, reuse it
    fetchBlockFromDatetimeUsingDb$({
      ctx: options.ctx,
      emitError: options.emitError,
      getBlockDate: options.getBlockDate,
    }),

    forkOnNullableField$({
      key: "blockNumber",
      handleNonNulls$: Rx.pipe(
        Rx.tap((item: { obj: TObj; blockNumber: unknown }) => {
          const blockNumber = item.blockNumber as number;
          logger.trace({
            msg: "Found block in database (datetime lookup)",
            data: { chain, datetime: options.getBlockDate(item.obj).toISOString(), blockNumber },
          });
        }),
        Rx.map((item: { obj: TObj; blockNumber: unknown }) => options.formatOutput(item.obj, item.blockNumber as number)),
      ),
      handleNulls$: Rx.pipe(
        Rx.tap((item: { obj: TObj; blockNumber: null }) =>
          logger.trace({
            msg: "Block not found in database (datetime lookup), using explorer",
            data: { chain, datetime: options.getBlockDate(item.obj).toISOString() },
          }),
        ),
        Rx.map((item: { obj: TObj; blockNumber: null }) => item.obj),

        // take a batch of items
        Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, options.ctx.streamConfig.maxInputTake),
        Rx.filter((objs) => objs.length > 0),

        // split by datetime (and chain but we are already in the context of a single chain)
        Rx.pipe(
          Rx.map((objs) => {
            const byDatetime = new Map<number, TObj[]>();
            for (const obj of objs) {
              const datetime = options.getBlockDate(obj).getTime();
              const list = byDatetime.get(datetime) || [];
              list.push(obj);
              byDatetime.set(datetime, list);
            }
            return Array.from(byDatetime.values());
          }),
          Rx.concatAll(),
          Rx.filter((objs) => objs.length > 0),
        ),

        // fetch block from etherscan api for the first item of the batch
        fetchBlockFromDatetimeUsingExplorerAPI$({
          ctx: options.ctx,
          emitError: (objs, report) => objs.forEach((obj) => options.emitError(obj, report)),
          getBlockDate: (objs: TObj[]) => options.getBlockDate(objs[0]),
          formatOutput: (objs, blockNumber) => ({ objs, blockNumber }),
        }),

        Rx.concatMap(({ objs, blockNumber }) => objs.map((obj) => options.formatOutput(obj, blockNumber))),
      ),
    }),
  );
}

function fetchBlockFromDatetimeUsingDb$<TObj, TErr extends ErrorEmitter<TObj>, TParams extends Date>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getBlockDate: (obj: TObj) => TParams;
}): Rx.OperatorFunction<TObj, { obj: TObj; blockNumber: number | null }> {
  const chain = options.ctx.chain;
  return dbBatchCall$({
    ctx: options.ctx,
    emitError: options.emitError,
    getData: (obj: TObj) => options.getBlockDate(obj).getTime(),
    logInfos: { msg: "fetch block from datetime (db lookup)", data: { chain } },
    formatOutput: (obj, blockNumber: number | null) => ({ obj, blockNumber } as { obj: TObj; blockNumber: number | null }),
    processBatch: async (objAndData) => {
      const dtMsList = objAndData.map((x) => x.data);
      const uniqDtMs = Array.from(new Set(dtMsList));

      const rows = await db_query<{ dtMs: number; blockNumber: number | null }>(
        `
        WITH req(datetime, dt_ms) AS (
          VALUES %L
        )
        SELECT
          req.dt_ms as "dtMs",
          b.block_number as "blockNumber"
        FROM req
        LEFT JOIN LATERAL (
          SELECT
            block_number
          FROM block_ts
          WHERE chain = %L
            and datetime between req.datetime - interval '120 seconds' and req.datetime + interval '120 seconds'
          ORDER BY abs(extract(epoch from (datetime - req.datetime))) ASC
          LIMIT 1
        ) b ON TRUE
        `,
        [uniqDtMs.map((dtMs) => [new Date(dtMs).toISOString(), dtMs]), chain],
        options.ctx.client,
      );

      const rowByDtMs = new Map<number, number | null>();
      for (const row of rows) {
        rowByDtMs.set(row.dtMs, row.blockNumber);
      }
      for (const dtMs of uniqDtMs) {
        if (!rowByDtMs.has(dtMs)) {
          rowByDtMs.set(dtMs, null);
        }
      }

      return new Map(dtMsList.map((dtMs) => [dtMs, rowByDtMs.get(dtMs) ?? null]));
    },
  });
}

function fetchBlockFromDatetimeUsingExplorerAPI$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends Date>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getBlockDate: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const chain = options.ctx.chain;

  const explorerConfig = EXPLORER_URLS[chain];
  if (explorerConfig.type !== "etherscan" && explorerConfig.type !== "etherscan-v2") {
    throw new ProgrammerError(`etherscan config is missing`);
  }
  return Rx.pipe(
    // fetch block from etherscan api
    // make sure we don't hit the rate limit of the explorers
    rateLimit$(samplingPeriodMs[options.ctx.behaviour.minDelayBetweenExplorerCalls]),

    Rx.mergeMap(async (obj) => {
      // https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=1695031435&closest=before
      // https://api.etherscan.io/v2/api?chainId=1&module=block&action=getblocknobytime&timestamp=1695031435&closest=before
      const timestamp = options.getBlockDate(obj).getTime() / 1000;
      let params = {
        chainId: getChainNetworkId(chain), // for etherscan v2 api endpoints
        module: "block",
        action: "getblocknobytime",
        timestamp,
        closest: "before",
        apiKey: undefined as string | undefined,
      };
      const apiKey = ETHERSCAN_API_KEY[chain];
      if (apiKey) {
        params.apiKey = apiKey;
      }
      logger.trace({ msg: "Fetching block from timestamp", data: { timestamp, params } });

      try {
        const resp = await axios.get(explorerConfig.url, { params });

        if (!resp.data || !resp.data.result) {
          logger.error({ msg: "No block number found", data: { timestamp, params, data: resp.data } });
          throw new Error("No block number found");
        }
        let blockNumber: number | string = resp.data.result;
        if (isString(blockNumber)) {
          blockNumber = parseInt(blockNumber);
        }

        if (!blockNumber) {
          logger.error({ msg: "No block number found", data: { timestamp, params, data: resp.data } });
          throw new Error("No block number found");
        }

        logger.trace({ msg: "Block number found", data: { timestamp, blockNumber, params } });

        return [options.formatOutput(obj, blockNumber)];
      } catch (error) {
        logger.error({ msg: "Error while fetching contract creation block", data: { params, error: error } });
        logger.error(error);
        options.emitError(obj, { error: error as Throwable, infos: { msg: "Error while fetching contract creation block" } });
        return Rx.EMPTY;
      }
    }, 1 /* concurrency */),

    Rx.concatAll(),
  );
}
