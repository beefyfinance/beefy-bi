import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { makeDataDirRecursive } from "./make-data-dir-recursive";

const CSV_SEPARATOR = ",";

export type SamplingPeriod = "15min"; // | "1hour" | "1day";
export const allSamplingPeriods = ["15min"];
export const samplingPeriodMs: { [period in SamplingPeriod]: number } = {
  "15min": 15 * 60 * 1000,
};

interface BlockSampleData {
  blockNumber: number;
  datetime: Date;
}
const blockSamplesColumns = ["blockNumber", "datetime"];

function getBlockSamplesFilePath(
  chain: Chain,
  samplingPeriod: SamplingPeriod
): string {
  return path.join(
    DATA_DIRECTORY,
    chain,
    "blocks",
    "samples",
    `${samplingPeriod}.csv`
  );
}

export async function getBlockSamplesStorageWriteStream(
  chain: Chain,
  samplingPeriod: SamplingPeriod
): Promise<{ writeBatch: (events: BlockSampleData[]) => Promise<void> }> {
  const filePath = getBlockSamplesFilePath(chain, samplingPeriod);
  await makeDataDirRecursive(filePath);
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });
  return {
    writeBatch: async (events) => {
      const csvData = stringifySync(events, {
        delimiter: CSV_SEPARATOR,
        cast: {
          date: (date) => date.toISOString(),
        },
      });
      writeStream.write(csvData);
    },
  };
}

export async function getLastImportedSampleBlockData(
  chain: Chain,
  samplingPeriod: SamplingPeriod
): Promise<BlockSampleData | null> {
  const filePath = getBlockSamplesFilePath(chain, samplingPeriod);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const lastImportedCSVRows = await readLastLines.read(filePath, 5);
  const data = syncParser(lastImportedCSVRows, {
    columns: blockSamplesColumns,
    cast: true,
    cast_date: true,
  });
  if (data.length === 0) {
    return null;
  }
  data.reverse();

  return data[0];
}
