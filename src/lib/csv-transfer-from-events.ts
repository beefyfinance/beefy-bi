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
import { onExit } from "../utils/process";

const CSV_SEPARATOR = ",";

export interface ERC20TransferFromEventData {
  blockNumber: number;
  datetime: Date;
  to: string;
  value: string;
}
const erc20TransferFromColumns = ["blockNumber", "datetime", "to", "value"];

function getContractERC20TransferFromFilePath(
  chain: Chain,
  fromContractAddress: string,
  tokenAddress: string
): string {
  return path.join(
    DATA_DIRECTORY,
    chain,
    "contracts",
    normalizeAddress(fromContractAddress),
    "ERC20_from_self",
    normalizeAddress(tokenAddress),
    "Transfer.csv"
  );
}

export async function getERC20TransferFromStorageWriteStream(
  chain: Chain,
  fromContractAddress: string,
  tokenAddress: string
): Promise<{
  writeBatch: (events: ERC20TransferFromEventData[]) => Promise<void>;
}> {
  const filePath = getContractERC20TransferFromFilePath(
    chain,
    fromContractAddress,
    tokenAddress
  );
  await makeDataDirRecursive(filePath);
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });

  let closed = false;
  onExit(async () => {
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

export async function getLastImportedERC20TransferFromBlockNumber(
  chain: Chain,
  fromContractAddress: string,
  tokenAddress: string
): Promise<number | null> {
  const filePath = getContractERC20TransferFromFilePath(
    chain,
    fromContractAddress,
    tokenAddress
  );
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const lastImportedCSVRows = await readLastLines.read(filePath, 5);
  const data = syncParser(lastImportedCSVRows, {
    delimiter: CSV_SEPARATOR,
    columns: erc20TransferFromColumns,
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
