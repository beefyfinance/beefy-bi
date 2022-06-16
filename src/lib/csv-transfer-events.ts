import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { logger } from "../utils/logger";

const CSV_SEPARATOR = ",";

export interface ERC20EventData {
  blockNumber: number;
  datetime: Date;
  from: string;
  to: string;
  value: string;
}
const erc20TransferColumns = ["blockNumber", "datetime", "from", "to", "value"];

function getContractERC20TransfersFilePath(
  chain: Chain,
  contractAddress: string
): string {
  return path.join(
    DATA_DIRECTORY,
    chain,
    "contracts",
    normalizeAddress(contractAddress),
    "ERC20",
    "Transfer.csv"
  );
}

export async function getERC20TransferStorageWriteStream(
  chain: Chain,
  contractAddress: string
): Promise<{ writeBatch: (events: ERC20EventData[]) => Promise<void> }> {
  const filePath = getContractERC20TransfersFilePath(chain, contractAddress);
  await makeDataDirRecursive(filePath);
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });

  let closed = false;
  process.on("SIGINT", function () {
    logger.info(`[ERC20.T.STORE] SIGINT, closing write stream`);
    closed = true;
    writeStream.close();
  });

  return {
    writeBatch: async (events) => {
      if (closed) {
        logger.debug(`[ERC20.T.STORE] stream closed, ignoring batch`);
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

export async function getLastImportedERC20TransferBlockNumber(
  chain: Chain,
  contractAddress: string
): Promise<number | null> {
  const filePath = getContractERC20TransfersFilePath(chain, contractAddress);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const lastImportedCSVRows = await readLastLines.read(filePath, 5);
  const data = syncParser(lastImportedCSVRows, {
    delimiter: CSV_SEPARATOR,
    columns: erc20TransferColumns,
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

  return parseInt(data[0].blockNumber);
}
