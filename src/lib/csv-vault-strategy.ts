import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { parse as asyncParser } from "csv-parse";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { logger } from "../utils/logger";
import { onExit } from "../utils/process";
import glob from "glob";

const CSV_SEPARATOR = ",";

export interface BeefyVaultV6StrategiesData {
  blockNumber: number;
  datetime: Date;
  implementation: string;
}
const beefyVaultStrategiesColumns = [
  "blockNumber",
  "datetime",
  "implementation",
];

function getBeefyVaultV6StrategiesFilePath(
  chain: Chain,
  contractAddress: string
): string {
  return path.join(
    DATA_DIRECTORY,
    "chain",
    chain,
    "contracts",
    normalizeAddress(contractAddress),
    "BeefyVaultV6",
    "strategies.csv"
  );
}

export async function getBeefyVaultV6StrategiesWriteStream(
  chain: Chain,
  contractAddress: string
): Promise<{
  writeBatch: (events: BeefyVaultV6StrategiesData[]) => Promise<void>;
}> {
  const filePath = getBeefyVaultV6StrategiesFilePath(chain, contractAddress);
  await makeDataDirRecursive(filePath);
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });

  let closed = false;
  onExit(async () => {
    if (closed) return;
    logger.info(`[VAULT.S.STORE] SIGINT, closing write stream`);
    closed = true;
    writeStream.close();
  });

  return {
    writeBatch: async (events) => {
      if (closed) {
        logger.debug(`[VAULT.S.STORE] stream closed, ignoring batch`);
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

export async function getLastImportedBeefyVaultV6Strategy(
  chain: Chain,
  contractAddress: string
): Promise<BeefyVaultV6StrategiesData | null> {
  const filePath = getBeefyVaultV6StrategiesFilePath(chain, contractAddress);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const lastImportedCSVRows = await readLastLines.read(filePath, 5);
  const data = syncParser(lastImportedCSVRows, {
    columns: beefyVaultStrategiesColumns,
    delimiter: CSV_SEPARATOR,
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
  });
  if (data.length === 0) {
    return null;
  }
  data.reverse();

  return data[0];
}

export async function* streamVaultStrategies(
  chain: Chain,
  contractAddress: string
) {
  const filePath = getBeefyVaultV6StrategiesFilePath(chain, contractAddress);
  if (!fs.existsSync(filePath)) {
    return;
  }
  const readStream: AsyncIterable<BeefyVaultV6StrategiesData> = fs
    .createReadStream(filePath)
    .pipe(
      asyncParser({
        delimiter: CSV_SEPARATOR,
        columns: beefyVaultStrategiesColumns,
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
  yield* readStream;
}

export async function* getAllStrategyAddresses(chain: Chain) {
  const filePaths = await new Promise<string[]>((resolve, reject) => {
    const globPath = path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      "*",
      "BeefyVaultV6",
      "strategies.csv"
    );
    // options is optional
    glob(globPath, function (er, filePaths) {
      if (er) {
        return reject(er);
      }
      resolve(filePaths);
    });
  });

  for (const filePath of filePaths) {
    const readStream: AsyncIterable<BeefyVaultV6StrategiesData> = fs
      .createReadStream(filePath)
      .pipe(
        asyncParser({
          delimiter: CSV_SEPARATOR,
          columns: beefyVaultStrategiesColumns,
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
      yield {
        ...record,
        // I forgot to normalize on write
        implementation: normalizeAddress(record.implementation),
      };
    }
  }
}
