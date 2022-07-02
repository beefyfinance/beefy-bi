import axios from "axios";
import * as fs from "fs";
import { Chain } from "../types/chain";
import { DATA_DIRECTORY, LOG_LEVEL } from "../utils/config";
import * as path from "path";
import { makeDataDirRecursive } from "./make-data-dir-recursive";
import { normalizeAddress } from "../utils/ethers";
import {
  ContractCreationInfo,
  fetchContractFirstLastTrxFromExplorer,
} from "./contract-transaction-infos";
import { cacheAsyncResultInRedis } from "../utils/cache";
import { getRedlock } from "./shared-resources/shared-lock";
import { backOff } from "exponential-backoff";
import { logger } from "../utils/logger";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import {
  BeefyFeeRecipientInfo,
  fetchBeefyStrategyFeeRecipients,
} from "../lib/strategy-fee-recipient-infos";

function fetchIfNotFoundLocally<TRes, TArgs extends any[]>(
  doFetch: (...parameters: TArgs) => Promise<TRes>,
  getLocalPath: (...parameters: TArgs) => string,
  options?: {
    ttl_ms?: number;
    getResourceId?: (...parameters: TArgs) => string;
    doWrite?: (data: TRes, filePath: string) => Promise<TRes>;
    doRead?: (filePath: string) => Promise<TRes>;
  }
) {
  // set default options
  const doWrite =
    options?.doWrite ||
    (async (data: TRes, filePath: string) => {
      await fs.promises.writeFile(filePath, JSON.stringify(data));
      return data;
    });
  const doRead =
    options?.doRead ||
    (async (filePath) => {
      const content = await fs.promises.readFile(filePath, "utf8");
      const data = JSON.parse(content);
      return data;
    });
  const getResourceId =
    options?.getResourceId ||
    (() => "fetchIfNotFoundLocally:" + Math.random().toString());

  const ttl = options?.ttl_ms || null;
  return async function (...parameters: TArgs): Promise<TRes> {
    const localPath = getLocalPath(...parameters);

    if (fs.existsSync(localPath)) {
      // resolve ttl
      let shouldServeCache = true;
      if (ttl !== null) {
        const now = new Date();
        const localStat = await fs.promises.stat(localPath);
        const lastModifiedDate = localStat.mtime;
        if (now.getTime() - lastModifiedDate.getTime() > ttl) {
          shouldServeCache = false;
        }
      }

      if (shouldServeCache) {
        logger.debug(
          `[FETCH] Local cache file exists and is not expired, returning it's content`
        );
        return doRead(localPath);
      } else {
        logger.debug(`[FETCH] Local cache file exists but is expired`);
      }
    }

    const redlock = await getRedlock();
    const resourceId = "fetch:" + getResourceId(...parameters);
    try {
      const data = await backOff(
        () =>
          redlock.using([resourceId], 2 * 60 * 1000, async () =>
            doFetch(...parameters)
          ),
        {
          delayFirstAttempt: false,
          jitter: "full",
          maxDelay: 5 * 60 * 1000,
          numOfAttempts: 10,
          retry: (error, attemptNumber) => {
            const message = `[FETCH] Error on attempt ${attemptNumber} fetching ${resourceId}: ${error.message}`;
            if (attemptNumber < 3) logger.verbose(message);
            else if (attemptNumber < 5) logger.info(message);
            else if (attemptNumber < 8) logger.warn(message);
            else logger.error(message);

            if (LOG_LEVEL === "trace") {
              console.error(error);
            }
            return true;
          },
          startingDelay: 200,
          timeMultiple: 2,
        }
      );
      logger.debug(
        `[FETCH] Got new data for ${resourceId}, writing it and returning it`
      );
      await makeDataDirRecursive(localPath);

      return doWrite(data, localPath);
    } catch (error) {
      if (fs.existsSync(localPath)) {
        logger.warn(
          `[FETCH] Could not reload local data after ttl expired: ${resourceId}. Serving local data anyway. ${JSON.stringify(
            error
          )}`
        );
        return doRead(localPath);
      } else {
        throw error;
      }
    }
  };
}

interface RawBeefyVault {
  id: string;
  tokenAddress?: string;
  tokenDecimals: number;
  earnContractAddress: string;
  earnedToken: string;
  isGovVault?: boolean;
  oracleId: string;
  status?: string;
  assets?: string[];
}
export interface BeefyVault {
  id: string;
  token_name: string;
  token_decimals: number;
  token_address: string;
  want_address: string;
  want_decimals: number;
  price_oracle: {
    want_oracleId: string;
    assets: string[];
  };
}
export const fetchBeefyVaultList = fetchIfNotFoundLocally(
  async (chain: Chain) => {
    logger.info(`[FETCH] Fetching updated vault list for ${chain}`);
    const res = await axios.get<RawBeefyVault[]>(
      `https://raw.githubusercontent.com/beefyfinance/beefy-v2/main/src/config/vault/${chain}.json`
    );
    const rawData: RawBeefyVault[] = res.data;
    const data: BeefyVault[] = rawData
      .filter((vault) => !(vault.isGovVault === true))
      .filter((vault) => !(vault.status === "eol"))
      .map((vault) => {
        try {
          const wnative = getChainWNativeTokenAddress(chain);
          return {
            id: vault.id,
            token_name: vault.earnedToken,
            token_decimals: 18,
            token_address: normalizeAddress(vault.earnContractAddress),
            want_address: normalizeAddress(vault.tokenAddress || wnative),
            want_decimals: vault.tokenDecimals,
            price_oracle: {
              want_oracleId: vault.oracleId,
              assets: vault.assets || [],
            },
          };
        } catch (error) {
          logger.debug(JSON.stringify({ vault, error }));
          throw error;
        }
      });
    return data;
  },
  (chain: Chain) =>
    path.join(DATA_DIRECTORY, "chain", chain, "beefy", "vaults.jsonl"),
  {
    ttl_ms: 1000 * 60 * 60 * 24,
    getResourceId: (chain: Chain) => `vault-list:${chain}`,
    doWrite: async (data, filePath) => {
      const jsonl = data.map((obj) => JSON.stringify(obj)).join("\n");
      await fs.promises.writeFile(filePath, jsonl);
      return data;
    },
    doRead: async (filePath) => {
      const content = await fs.promises.readFile(filePath, "utf8");
      const data = content.split("\n").map((obj) => JSON.parse(obj));
      return data;
    },
  }
);

export async function getLocalBeefyVaultList(
  chain: Chain
): Promise<BeefyVault[]> {
  const filePath = path.join(
    DATA_DIRECTORY,
    "chain",
    chain,
    "beefy",
    "vaults.jsonl"
  );
  if (!fs.existsSync(filePath)) {
    return [];
  }
  const content = await fs.promises.readFile(filePath, "utf8");
  const data = content.split("\n").map((obj) => JSON.parse(obj));
  return data;
}

export const fetchContractCreationInfos = fetchIfNotFoundLocally(
  async (
    chain: Chain,
    contractAddress: string
  ): Promise<ContractCreationInfo> => {
    return fetchContractFirstLastTrxFromExplorer(
      chain,
      contractAddress,
      "first"
    );
  },
  (chain: Chain, contractAddress: string) =>
    path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(contractAddress),
      "creation_date.json"
    )
);

export async function getLocalContractCreationInfos(
  chain: Chain,
  contractAddress: string
): Promise<ContractCreationInfo | null> {
  const filePath = path.join(
    DATA_DIRECTORY,
    "chain",
    chain,
    "contracts",
    normalizeAddress(contractAddress),
    "creation_date.json"
  );
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const content = await fs.promises.readFile(filePath, "utf8");
  const data = JSON.parse(content);
  return data;
}

export const fetchCachedContractLastTransaction = cacheAsyncResultInRedis(
  async (
    chain: Chain,
    contractAddress: string
  ): Promise<ContractCreationInfo> => {
    return fetchContractFirstLastTrxFromExplorer(
      chain,
      contractAddress,
      "last"
    );
  },
  {
    getKey: (chain: Chain, contractAddress: string) =>
      `${chain}:${contractAddress}:last-trx`,
    dateFields: ["datetime"],
    ttl_sec: 60 * 60,
  }
);

export const getBeefyStrategyFeeRecipients = fetchIfNotFoundLocally(
  async (
    chain: Chain,
    contractAddress: string
  ): Promise<BeefyFeeRecipientInfo> => {
    return fetchBeefyStrategyFeeRecipients(chain, contractAddress);
  },
  (chain: Chain, contractAddress: string) =>
    path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(contractAddress),
      "fee_recipients.json"
    )
);

export async function getLocalBeefyStrategyFeeRecipients(
  chain: Chain,
  contractAddress: string
): Promise<BeefyFeeRecipientInfo | null> {
  const filePath = path.join(
    DATA_DIRECTORY,
    "chain",
    chain,
    "contracts",
    normalizeAddress(contractAddress),
    "fee_recipients.json"
  );
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const content = await fs.promises.readFile(filePath, "utf8");
  const data = JSON.parse(content);
  return data;
}
