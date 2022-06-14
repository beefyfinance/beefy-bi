import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { makeDataDirRecursive } from "./make-data-dir-recursive";

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
    chain,
    "contracts",
    normalizeAddress(contractAddress),
    "BeefyVaultV6",
    `strategies.csv`
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
