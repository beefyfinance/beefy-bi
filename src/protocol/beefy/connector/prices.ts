import axios from "axios";
import * as Rx from "rxjs";
import { SamplingPeriod } from "../../../types/sampling";
import { BEEFY_DATA_URL } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { Range } from "../../../utils/range";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { ErrorEmitter, ImportCtx } from "../../common/types/import-context";

export interface PriceSnapshot {
  oracleId: string;
  datetime: Date;
  value: number;
}
const logger = rootLogger.child({ module: "beefy", component: "prices" });

interface BeefyPriceCallParams {
  oracleId: string;
  samplingPeriod: SamplingPeriod;
  range: Range<Date>;
}

export function fetchBeefyDataPrices$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TParams extends BeefyPriceCallParams>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getPriceParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, prices: PriceSnapshot[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    // be nice with beefy api plz
    rateLimit$(300),

    // now we fetch
    Rx.concatMap(async (item) => {
      const params = options.getPriceParams(item);

      logger.debug({ msg: "fetching prices", data: params });
      try {
        const prices = await fetchBeefyPrices(params.samplingPeriod, params.oracleId, {
          startDate: params.range.from,
          endDate: params.range.to,
        });
        logger.debug({ msg: "got prices", data: { ...params, priceCount: prices.length } });

        return Rx.of(options.formatOutput(item, prices));
      } catch (error) {
        logger.error({ msg: "error fetching prices", data: { ...params, error } });
        logger.error(error);
        options.emitError(item);
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
