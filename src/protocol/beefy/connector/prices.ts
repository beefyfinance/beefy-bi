import axios from "axios";
import * as Rx from "rxjs";
import { RpcConfig } from "../../../types/rpc-config";
import { SamplingPeriod } from "../../../types/sampling";
import { BEEFY_DATA_URL } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { Range } from "../../../utils/range";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { ErrorEmitter, ImportQuery } from "../../common/types/import-query";
import { BatchStreamConfig } from "../../common/utils/batch-rpc-calls";

export interface PriceSnapshot {
  oracleId: string;
  datetime: Date;
  value: number;
}
const logger = rootLogger.child({ module: "beefy", component: "prices" });

interface BeefyPriceCallParams {
  oracleId: string;
  samplingPeriod: SamplingPeriod;
}

export function fetchBeefyDataPrices$<
  TTarget,
  TObj extends ImportQuery<TTarget, Date>,
  TParams extends BeefyPriceCallParams,
  TRes extends ImportQuery<TTarget, Date>,
>(options: {
  getPriceParams: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter<TTarget, Date>;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, prices: PriceSnapshot[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // be nice with beefy api plz
    rateLimit$(300),

    // now we fetch
    Rx.concatMap(async (item) => {
      const params = options.getPriceParams(item);
      const debugLogData = {
        oracleId: params.oracleId,
        range: item.range,
      };
      logger.debug({ msg: "fetching prices", data: debugLogData });
      try {
        const prices = await fetchBeefyPrices(params.samplingPeriod, params.oracleId, {
          startDate: item.range.from,
          endDate: item.range.to,
        });
        logger.debug({ msg: "got prices", data: { ...debugLogData, priceCount: prices.length } });

        return Rx.of(options.formatOutput(item, prices));
      } catch (error) {
        logger.error({ msg: "error fetching prices", data: { ...debugLogData, error } });
        logger.error(error);
        options.emitErrors(item);
        return Rx.EMPTY;
      }
    }),
    Rx.concatAll(),
  );
}

export async function fetchBeefyPrices(
  samplingPeriod: SamplingPeriod,
  oracleId: string,
  options?: {
    startDate?: Date;
    endDate?: Date;
  },
) {
  if (samplingPeriod !== "15min") {
    throw new Error(`Unsupported sampling period: ${samplingPeriod}`);
  }
  const apiPeriod = samplingPeriod === "15min" ? "minute" : "minute";
  const startDate = options?.startDate || new Date(0);
  const endDate = options?.endDate || new Date(new Date().getTime() + 10000000);
  logger.debug({ msg: "Fetching prices", data: { oracleId } });

  const res = await axios.get<{ ts: number; name: string; v: number }[]>(BEEFY_DATA_URL + "/price", {
    params: {
      name: oracleId,
      period: apiPeriod,
      from: Math.floor(startDate.getTime() / 1000),
      to: Math.ceil(endDate.getTime() / 1000),
      limit: 1000000000,
    },
  });

  return res.data.map(
    (price) =>
      ({
        datetime: new Date(price.ts),
        oracleId: price.name,
        value: price.v,
      } as PriceSnapshot),
  );
}
