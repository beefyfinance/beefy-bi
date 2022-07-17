import * as path from "path";
import { Chain } from "../../types/chain";
import { DATA_DIRECTORY } from "../../utils/config";
import { CsvStore } from "../../utils/csv-store";
import { SamplingPeriod } from "../../types/sampling";

interface BlockSampleData {
  blockNumber: number;
  datetime: Date;
}
export const blockSamplesStore = new CsvStore<BlockSampleData, [Chain, SamplingPeriod]>({
  dateFieldPosition: 1,
  loggerScope: "BlocksStore",
  getFilePath: (chain: Chain, samplingPeriod: SamplingPeriod) =>
    path.join(DATA_DIRECTORY, "chain", chain, "blocks", "samples", `${samplingPeriod}.csv`),
  csvColumns: [
    { name: "blockNumber", type: "integer" },
    { name: "datetime", type: "date" },
  ],
});
