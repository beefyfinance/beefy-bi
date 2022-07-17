import * as path from "path";
import { DATA_DIRECTORY } from "../../utils/config";
import { SamplingPeriod } from "../../types/sampling";
import { glob } from "glob";
import { CsvStore } from "../../utils/csv-store";

export interface OraclePriceData {
  datetime: Date;
  usdValue: number;
}

class PriceCsvStore extends CsvStore<OraclePriceData, [string, SamplingPeriod]> {
  async *getAllAvailableOracleIds(samplingPeriod: SamplingPeriod) {
    const filePaths = await new Promise<string[]>((resolve, reject) => {
      const globPath = path.join(DATA_DIRECTORY, "price", "beefy", "*", `price_${samplingPeriod}.csv`);

      // options is optional
      glob(globPath, function (er, filePaths) {
        if (er) {
          return reject(er);
        }
        resolve(filePaths);
      });
    });

    yield* filePaths.map((p) => p.split("/")).map((pp) => pp[pp.length - 2]);
  }
}

export const oraclePriceStore = new PriceCsvStore({
  dateFieldPosition: 0,
  loggerScope: "OraclePriceStore",
  getFilePath: (oracleId: string, samplingPeriod: SamplingPeriod) =>
    path.join(DATA_DIRECTORY, "price", "beefy", oracleId, `price_${samplingPeriod}.csv`),
  csvColumns: [
    { name: "datetime", type: "date" },
    { name: "usdValue", type: "float" },
  ],
});
