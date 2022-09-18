import axios from "axios";
import { BEEFY_DATA_URL } from "../../../utils/config";
import { SamplingPeriod } from "../../../types/sampling";
import { rootLogger } from "../../../utils/logger";

interface PriceSnapshot {
  oracleId: string;
  datetime: Date;
  value: number;
}
const logger = rootLogger.child({ module: "beefy", component: "prices" });

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
