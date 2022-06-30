import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { parse as asyncParser } from "csv-parse";
import { DATA_DIRECTORY } from "../utils/config";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { logger } from "../utils/logger";
import { onExit } from "../utils/process";
import { SamplingPeriod } from "./csv-block-samples";

const CSV_SEPARATOR = ",";

export interface OraclePriceData {
  datetime: Date;
  usdValue: number;
}
const oraclePriceColumns = ["datetime", "usdValue"];

function getOraclePriceFilePath(
  oracleId: string,
  samplingPeriod: SamplingPeriod
): string {
  return path.join(
    DATA_DIRECTORY,
    "price",
    "beefy",
    oracleId,
    `price_${samplingPeriod}.csv`
  );
}

export async function getOraclePriceWriteStream(
  oracleId: string,
  samplingPeriod: SamplingPeriod
): Promise<{
  writeBatch: (events: OraclePriceData[]) => Promise<void>;
  close: () => Promise<any>;
}> {
  const filePath = getOraclePriceFilePath(oracleId, samplingPeriod);
  await makeDataDirRecursive(filePath);
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });

  let closed = false;
  onExit(async () => {
    if (closed) return;
    logger.info(`[PRICE.STORE] SIGINT, closing write stream`);
    closed = true;
    writeStream.close();
  });

  return {
    writeBatch: async (events) => {
      if (closed) {
        logger.debug(`[PRICE.STORE] stream closed, ignoring batch`);
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
    close: async () => {
      if (closed) return;
      logger.verbose(`[PRICE.STORE] closing write stream`);
      closed = true;
      writeStream.close();
    },
  };
}

export async function getLastImportedOraclePrice(
  oracleId: string,
  samplingPeriod: SamplingPeriod
): Promise<OraclePriceData | null> {
  const filePath = getOraclePriceFilePath(oracleId, samplingPeriod);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const lastImportedCSVRows = await readLastLines.read(filePath, 5);
  const data = syncParser(lastImportedCSVRows, {
    columns: oraclePriceColumns,
    delimiter: CSV_SEPARATOR,
    cast: (value, context) => {
      if (context.index === 0) {
        return new Date(value);
      } else if (context.index === 1) {
        return parseFloat(value);
      } else {
        return value;
      }
    },
    cast_date: true,
  });
  if (data.length === 0) {
    return null;
  }
  data.reverse();

  return data[0];
}

export async function getFirstImportedOraclePrice(
  oracleId: string,
  samplingPeriod: SamplingPeriod
): Promise<OraclePriceData | null> {
  const readStream = streamOraclePrices(oracleId, samplingPeriod);
  for await (const event of readStream) {
    return event;
  }
  return null;
}

export async function* streamOraclePrices(
  oracleId: string,
  samplingPeriod: SamplingPeriod
): AsyncIterable<OraclePriceData> {
  const filePath = getOraclePriceFilePath(oracleId, samplingPeriod);
  if (!fs.existsSync(filePath)) {
    return;
  }
  const readStream = fs.createReadStream(filePath).pipe(
    asyncParser({
      delimiter: CSV_SEPARATOR,
      columns: oraclePriceColumns,
      cast: (value, context) => {
        if (context.index === 0) {
          return new Date(value);
        } else if (context.index === 1) {
          return parseFloat(value);
        } else {
          return value;
        }
      },
      cast_date: true,
    })
  );
  yield* readStream;

  readStream.destroy();
}
