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
import { SamplingPeriod } from "../types/sampling";
import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { ethers } from "ethers";
import {
  ArchiveNodeNeededError,
  callLockProtectedRpc,
  isErrorDueToMissingDataFromNode,
} from "./shared-resources/shared-rpc";
import { logger } from "../utils/logger";
import { onExit } from "../utils/process";
import axios from "axios";
import { sortBy } from "lodash";

const CSV_SEPARATOR = ",";

export interface BeefyVaultV6PPFSData {
  blockNumber: number;
  datetime: Date;
  pricePerFullShare: string;
}
const beefyVaultPPFSColumns = ["blockNumber", "datetime", "pricePerFullShare"];

function getBeefyVaultV6PPFSFilePath(
  chain: Chain,
  contractAddress: string,
  samplingPeriod: SamplingPeriod
): string {
  return path.join(
    DATA_DIRECTORY,
    "chain",
    chain,
    "contracts",
    normalizeAddress(contractAddress),
    "BeefyVaultV6",
    `ppfs_${samplingPeriod}.csv`
  );
}

export async function getBeefyVaultV6PPFSWriteStream(
  chain: Chain,
  contractAddress: string,
  samplingPeriod: SamplingPeriod
): Promise<{
  writeBatch: (events: BeefyVaultV6PPFSData[]) => Promise<void>;
  close: () => Promise<void>;
}> {
  const filePath = getBeefyVaultV6PPFSFilePath(
    chain,
    contractAddress,
    samplingPeriod
  );
  await makeDataDirRecursive(filePath);

  logger.debug(
    `[VAULT.PPFS.STORE] Opening write stream for ${chain}:${contractAddress}:${samplingPeriod}`
  );
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });

  let closed = false;
  onExit(async () => {
    if (closed) return;
    logger.info(`[VAULT.PPFS.STORE] SIGINT, closing write stream`);
    closed = true;
    writeStream.close();
  });

  return {
    writeBatch: async (events) => {
      if (closed) {
        logger.debug(`[VAULT.PPFS.STORE] stream closed, ignoring batch`);
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
      if (closed) {
        logger.warn(`[VAULT.PPFS.STORE] stream already closed`);
      }
      logger.debug(
        `[VAULT.PPFS.STORE] closing write stream for ${chain}:${contractAddress}:${samplingPeriod}`
      );
      closed = true;
      writeStream.close();
    },
  };
}

export async function getLastImportedBeefyVaultV6PPFSData(
  chain: Chain,
  contractAddress: string,
  samplingPeriod: SamplingPeriod
): Promise<BeefyVaultV6PPFSData | null> {
  const filePath = getBeefyVaultV6PPFSFilePath(
    chain,
    contractAddress,
    samplingPeriod
  );
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const lastImportedCSVRows = await readLastLines.read(filePath, 5);
  const data = syncParser(lastImportedCSVRows, {
    columns: beefyVaultPPFSColumns,
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

export async function getFirstImportedBeefyVaultV6PPFSData(
  chain: Chain,
  contractAddress: string,
  samplingPeriod: SamplingPeriod
): Promise<BeefyVaultV6PPFSData | null> {
  const readStream = streamBeefyVaultV6PPFSData(
    chain,
    contractAddress,
    samplingPeriod
  );
  for await (const event of readStream) {
    return event;
  }
  return null;
}

export function getBeefyVaultV6PPFSDataStream(
  chain: Chain,
  contractAddress: string,
  samplingPeriod: SamplingPeriod
) {
  const filePath = getBeefyVaultV6PPFSFilePath(
    chain,
    contractAddress,
    samplingPeriod
  );
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const readStream = fs.createReadStream(filePath).pipe(
    asyncParser({
      delimiter: CSV_SEPARATOR,
      columns: beefyVaultPPFSColumns,
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
  return readStream;
}

export async function* streamBeefyVaultV6PPFSData(
  chain: Chain,
  contractAddress: string,
  samplingPeriod: SamplingPeriod
): AsyncIterable<BeefyVaultV6PPFSData> {
  const readStream = getBeefyVaultV6PPFSDataStream(
    chain,
    contractAddress,
    samplingPeriod
  );
  if (!readStream) {
    return;
  }
  yield* readStream;

  readStream.destroy();
}

export async function fetchBeefyPPFS(
  chain: Chain,
  contractAddress: string,
  blockNumbers: number[]
): Promise<ethers.BigNumber[]> {
  const ppfsPromises = await callLockProtectedRpc(chain, async (provider) => {
    const contract = new ethers.Contract(
      contractAddress,
      BeefyVaultV6Abi,
      provider
    );

    logger.debug(
      `[PPFS] Batch fetching PPFS for ${chain}:${contractAddress} (${
        blockNumbers[0]
      } -> ${blockNumbers[blockNumbers.length - 1]}) (${
        blockNumbers.length
      } blocks)`
    );
    // it looks like ethers doesn't yet support harmony's special format or smth
    // same for heco
    if (chain === "harmony" || chain === "heco") {
      return fetchBeefyPPFSWithManualRPCCall(
        provider,
        chain,
        contractAddress,
        blockNumbers
      );
    }
    return blockNumbers.map((blockNumber) => {
      return contract.functions.getPricePerFullShare({
        // a block tag to simulate the execution at, which can be used for hypothetical historic analysis;
        // note that many backends do not support this, or may require paid plans to access as the node
        // database storage and processing requirements are much higher
        blockTag: blockNumber,
      }) as Promise<[ethers.BigNumber]>;
    });
  });
  const ppfs = await Promise.all(ppfsPromises);
  return ppfs.map(([ppfs]) => ppfs);
}

/**
 * I don't know why this is needed but seems like ethers.js is not doing the right rpc call
 */
async function fetchBeefyPPFSWithManualRPCCall(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractAddress: string,
  blockNumbers: number[]
): Promise<Promise<[ethers.BigNumber]>[]> {
  const url = provider.connection.url;

  // get the function call hash
  const abi = ["function getPricePerFullShare()"];
  const iface = new ethers.utils.Interface(abi);
  const callData = iface.encodeFunctionData("getPricePerFullShare");

  // somehow block tag has to be hex encoded for heco
  const batchParams = blockNumbers.map((blockNumber, idx) => ({
    method: "eth_call",
    params: [
      {
        from: null,
        to: contractAddress,
        data: callData,
      },
      ethers.utils.hexValue(blockNumber),
    ],
    id: idx,
    jsonrpc: "2.0",
  }));

  type BatchResItem =
    | {
        jsonrpc: "2.0";
        id: number;
        result: string;
      }
    | {
        jsonrpc: "2.0";
        id: number;
        error: string;
      };
  const results = await axios.post<BatchResItem[]>(url, batchParams);
  return sortBy(results.data, (res) => res.id).map((res) => {
    if (isErrorDueToMissingDataFromNode(res)) {
      throw new ArchiveNodeNeededError(chain, res);
    } else if ("error" in res) {
      throw new Error("Error in fetching PPFS: " + JSON.stringify(res));
    }
    const ppfs = ethers.utils.defaultAbiCoder.decode(
      ["uint256"],
      res.result
    ) as any as [ethers.BigNumber];
    return Promise.resolve(ppfs);
  });
}
