import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { parse as asyncParser } from "csv-parse";
import { stringify as stringifySync } from "csv-stringify/sync";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { logger } from "../utils/logger";
import { onExit } from "../utils/process";

const CSV_SEPARATOR = ",";

export type SamplingPeriod = "15min" | "1hour" | "4hour" | "1day";
export const allSamplingPeriods = ["15min", "1hour", "4hour", "1day"];
export const samplingPeriodMs: { [period in SamplingPeriod]: number } = {
  "15min": 15 * 60 * 1000,
  "1hour": 60 * 60 * 1000,
  "4hour": 4 * 60 * 60 * 1000,
  "1day": 24 * 60 * 60 * 1000,
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

  let closed = false;
  onExit(async () => {
    logger.info(`[BLOCK_STORE] SIGINT, closing write stream`);
    closed = true;
    writeStream.close();
  });

  return {
    writeBatch: async (events) => {
      if (closed) {
        logger.debug(`[BLOCK_STORE] stream closed, ignoring batch`);
        return;
      }
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

export async function* streamBlockSamplesFrom(
  chain: Chain,
  samplingPeriod: SamplingPeriod,
  fromBlock: number
) {
  const filePath = getBlockSamplesFilePath(chain, samplingPeriod);
  const readStream: AsyncIterable<BlockSampleData> = fs
    .createReadStream(filePath)
    .pipe(
      asyncParser({
        delimiter: CSV_SEPARATOR,
        columns: blockSamplesColumns,
        cast: (value, context) => {
          if (context.index === 0) {
            return parseInt(value);
          } else if (context.index === 1) {
            return new Date(value);
          } else {
            return value;
          }
        },
        cast_date: true,
      })
    );
  for await (const record of readStream) {
    if (record.blockNumber >= fromBlock) {
      yield record;
    }
  }
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
    delimiter: CSV_SEPARATOR,
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
