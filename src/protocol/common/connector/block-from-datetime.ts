import axios from "axios";
import { isString } from "lodash";
import * as Rx from "rxjs";
import { samplingPeriodMs } from "../../../types/sampling";
import { EXPLORER_URLS } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { ErrorEmitter, ImportCtx, Throwable } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "block-datetime" });

export function fetchBlockFromDatetime$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends Date>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getBlockDate: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, blockNumber: number) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const chain = options.ctx.chain;

  if (!options.ctx.rpcConfig.etherscan) {
    throw new ProgrammerError(`etherscan config is missing`);
  }

  const explorerConfig = EXPLORER_URLS[chain];
  if (explorerConfig.type !== "etherscan") {
    throw new ProgrammerError(`etherscan config is missing`);
  }

  return Rx.pipe(
    // add TS typings
    Rx.tap((_: TObj) => {}),

    // TODO: fetch block from db if it exists and is close enough

    // fetch block from etherscan api

    // make sure we don't hit the rate limit of the explorers
    rateLimit$(samplingPeriodMs[options.ctx.behaviour.minDelayBetweenExplorerCalls]),

    Rx.mergeMap(async (obj) => {
      // https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp=1695031435&closest=before
      const timestamp = options.getBlockDate(obj).getTime() / 1000;
      const params = {
        module: "block",
        action: "getblocknobytime",
        timestamp,
        closest: "before",
      };
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
