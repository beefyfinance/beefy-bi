import axios from "axios";
import * as Rx from "rxjs";
import { SamplingPeriod } from "../../../types/sampling";
import { BEEFY_DATA_KEY, BEEFY_DATA_URL } from "../../../utils/config";
import { rootLogger } from "../../../utils/logger";
import { Range } from "../../../utils/range";
import { rateLimit$ } from "../../../utils/rxjs/utils/rate-limit";
import { ErrorEmitter, ErrorReport, ImportCtx } from "../../common/types/import-context";

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
      } catch (error: any) {
        const report: ErrorReport = { error, infos: { msg: "error fetching prices", data: { ...params } } };
        logger.debug(report.infos);
        logger.debug(report.error);
        options.emitError(item, report);
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
  const startDate = options?.startDate || new Date(0);
  const endDate = options?.endDate || new Date(new Date().getTime() + 10000000);

  const params = {
    oracle: oracleId,
    from: Math.floor(startDate.getTime() / 1000),
    to: Math.ceil(endDate.getTime() / 1000),
    key: BEEFY_DATA_KEY,
  };
  logger.debug({ msg: "Fetching prices", data: { params } });

  const res = await axios.get<{ t: number; v: number }[]>(BEEFY_DATA_URL + "/api/v2/prices/range", { params });

  return res.data.map(
    (price) =>
      ({
        datetime: new Date(price.t * 1000),
        oracleId: oracleId,
        value: price.v,
      } as PriceSnapshot),
  );
}
