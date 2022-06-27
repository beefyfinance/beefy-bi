import * as fs from "fs";
import * as path from "path";
import * as readLastLines from "read-last-lines";
import { parse as syncParser } from "csv-parse/sync";
import { stringify as stringifySync } from "csv-stringify/sync";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { SamplingPeriod } from "./csv-block-samples";
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
): Promise<{ writeBatch: (events: BeefyVaultV6PPFSData[]) => Promise<void> }> {
  const filePath = getBeefyVaultV6PPFSFilePath(
    chain,
    contractAddress,
    samplingPeriod
  );
  await makeDataDirRecursive(filePath);
  const writeStream = fs.createWriteStream(filePath, { flags: "a" });

  let closed = false;
  onExit(async () => {
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

export async function fetchBeefyPPFS(
  chain: Chain,
  contractAddress: string,
  blockNumber: number
): Promise<ethers.BigNumber> {
  // it looks like ethers doesn't yet support harmony's special format or smth
  // same for heco
  if (chain === "harmony" || chain === "heco") {
    return fetchBeefyPPFSWithManualRPCCall(chain, contractAddress, blockNumber);
  }

  logger.debug(
    `[PPFS] Fetching PPFS for ${chain}:${contractAddress}:${blockNumber}`
  );
  return callLockProtectedRpc(chain, async (provider) => {
    const contract = new ethers.Contract(
      contractAddress,
      BeefyVaultV6Abi,
      provider
    );
    const ppfs: [ethers.BigNumber] =
      await contract.functions.getPricePerFullShare({
        // a block tag to simulate the execution at, which can be used for hypothetical historic analysis;
        // note that many backends do not support this, or may require paid plans to access as the node
        // database storage and processing requirements are much higher
        blockTag: blockNumber,
      });
    return ppfs[0];
  });
}

/**
 * I don't know why this is needed but seems like ethers.js is not doing the right rpc call
 */
async function fetchBeefyPPFSWithManualRPCCall(
  chain: Chain,
  contractAddress: string,
  blockNumber: number
): Promise<ethers.BigNumber> {
  logger.debug(
    `[PPFS] Fetching PPFS for ${chain}:${contractAddress}:${blockNumber}`
  );
  return callLockProtectedRpc(chain, async (provider) => {
    const url = provider.connection.url;

    // get the function call hash
    const abi = ["function getPricePerFullShare()"];
    const iface = new ethers.utils.Interface(abi);
    const callData = iface.encodeFunctionData("getPricePerFullShare");

    // somehow block tag has to be hex encoded for heco
    const blockNumberHex = ethers.utils.hexValue(blockNumber);

    const res = await axios.post(url, {
      method: "eth_call",
      params: [
        {
          from: null,
          to: contractAddress,
          data: callData,
        },
        blockNumberHex,
      ],
      id: 1,
      jsonrpc: "2.0",
    });

    if (isErrorDueToMissingDataFromNode(res.data)) {
      throw new ArchiveNodeNeededError(chain, res.data);
    }
    const ppfs = ethers.utils.defaultAbiCoder.decode(
      ["uint256"],
      res.data.result
    ) as any as [ethers.BigNumber];
    return ppfs[0];
  });
}
