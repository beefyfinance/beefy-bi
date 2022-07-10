import * as path from "path";
import * as fs from "fs";
import { simpleGit, SimpleGit, SimpleGitOptions } from "simple-git";
import { Chain } from "../types/chain";
import { GIT_WORK_DIRECTORY } from "../utils/config";
import { logger } from "../utils/logger";
import { sortBy } from "lodash";
import { normalizeAddress } from "../utils/ethers";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import prettier from "prettier";

interface RawBeefyVault {
  id: string;
  tokenAddress?: string;
  tokenDecimals: number;
  earnedTokenAddress: string;
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
  eol: boolean;
  is_gov_vault: boolean;
  price_oracle: {
    want_oracleId: string;
    assets: string[];
  };
}

export async function getAllVaultsFromGitHistory(
  chain: Chain
): Promise<BeefyVault[]> {
  logger.info(`[GIT.V] Fetching updated vault list for ${chain}`);

  const vaultsByAddress: Record<string, RawBeefyVault> = {};

  // import v2 vaults first
  logger.verbose(`[GIT.V] Fetching vault list for ${chain} from v2`);
  const fileContentStreamV2 = gitStreamFileVersions({
    remote: "git@github.com:beefyfinance/beefy-v2.git",
    branch: "main",
    filePath: `src/config/vault/${chain}.json`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v2"),
    order: "recent-to-old",
    throwOnError: false,
  });
  for await (const fileVersion of fileContentStreamV2) {
    const vaults: RawBeefyVault[] = JSON.parse(fileVersion.fileContent);

    for (const vault of vaults) {
      if (!vault.earnedTokenAddress) {
        logger.error(
          `[GIT.V] Could not find vault earned token address for v2 vault`
        );
        console.log(vault);
        continue;
      }
      const earnedTokenAddress = normalizeAddress(vault.earnedTokenAddress);
      if (!vaultsByAddress[earnedTokenAddress]) {
        vaultsByAddress[earnedTokenAddress] = vault;
      }
    }
  }

  // then v1 vaults
  logger.verbose(`[GIT.V] Fetching vault list for ${chain} from v1`);
  const v1Chain = chain === "avax" ? "avalanche" : chain;
  const fileContentStreamV1 = gitStreamFileVersions({
    remote: "git@github.com:beefyfinance/beefy-app.git",
    branch: "prod",
    filePath: `src/features/configure/vault/${v1Chain}_pools.js`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v1"),
    order: "recent-to-old",
    throwOnError: false,
  });

  for await (const fileVersion of fileContentStreamV1) {
    // using prettier to transform js objects into proper json was the easiest way for me
    try {
      const jsonContent = prettier.format(
        "[" +
          fileVersion.fileContent
            .replace(/\/\*(\s|\S)*?\*\//gm, "") // remove comments
            .trim()
            .split("\n")
            .slice(1, -1)
            .join("\n") +
          "]",
        { semi: false, parser: "json" }
      );
      const vaults: RawBeefyVault[] = JSON.parse(jsonContent);

      for (const vault of vaults) {
        if (!vault.earnedTokenAddress) {
          logger.error(
            `[GIT.V] Could not find vault earned token address for v1 vault`
          );
          console.log(vault);
          continue;
        }
        const earnedTokenAddress = normalizeAddress(vault.earnedTokenAddress);
        if (!vaultsByAddress[earnedTokenAddress]) {
          vaultsByAddress[earnedTokenAddress] = vault;
        }
      }
    } catch (e) {
      logger.error(
        `[GIT.V] Could not parse vault list for v1 hash ${chain}:${fileVersion.commitHash}:${fileVersion.date}`
      );
      console.log(e);
    }
  }

  logger.verbose(`[GIT.V] All raw vaults found for ${chain} mapping to schema`);
  const vaults = Object.values(vaultsByAddress).map((rawVault) => {
    try {
      const wnative = getChainWNativeTokenAddress(chain);
      return {
        id: rawVault.id,
        token_name: rawVault.earnedToken,
        token_decimals: 18,
        token_address: normalizeAddress(rawVault.earnContractAddress),
        want_address: normalizeAddress(rawVault.tokenAddress || wnative),
        want_decimals: rawVault.tokenDecimals,
        eol: rawVault.status === "eol",
        is_gov_vault: rawVault.isGovVault || false,
        price_oracle: {
          want_oracleId: rawVault.oracleId,
          assets: rawVault.assets || [],
        },
      };
    } catch (error) {
      logger.debug(JSON.stringify({ vault: rawVault, error }));
      throw error;
    }
  });
  logger.info(`[GIT.V] Fetched ${vaults.length} vaults`);
  return vaults;
}

export async function* gitStreamFileVersions(options: {
  remote: string;
  workdir: string;
  branch: string;
  filePath: string;
  order: "recent-to-old" | "old-to-recent";
  throwOnError: boolean;
}): AsyncGenerator<{ commitHash: string; date: Date; fileContent: string }> {
  const baseOptions: Partial<SimpleGitOptions> = {
    binary: "git",
    maxConcurrentProcesses: 6,
  };

  // pull latest changes from remote or just clone remote
  if (!fs.existsSync(options.workdir)) {
    logger.debug(`[GIT.V] cloning ${options.remote} into ${options.workdir}`);
    const git: SimpleGit = simpleGit({
      ...baseOptions,
      baseDir: GIT_WORK_DIRECTORY,
    });
    await git.clone(options.remote, options.workdir);
  } else {
    logger.debug(`[GIT.V] Local repo found at ${options.workdir}.`);
  }

  // get a new git instance for the current git repo
  const git = simpleGit({
    ...baseOptions,
    baseDir: options.workdir,
  });
  // switch to the target branch
  logger.debug(`[GIT.V] Changing branch to ${options.branch}`);
  await git.checkout(options.branch);
  logger.debug(`[GIT.V] Pulling changes for branch ${options.branch}`);
  await git.pull("origin", options.branch);

  // get all commit hashes for the target file
  logger.debug(`[GIT.V] Pulling all commit hash for file ${options.filePath}`);
  const log = await git.log({
    file: options.filePath,
    format: "%H",
  });
  let logs = log.all as any as { hash: string; date: string }[];

  if (options.order === "old-to-recent") {
    logs = sortBy(logs, (log) => log.date);
  } else if (options.order === "recent-to-old") {
    logs = sortBy(logs, (log) => log.date).reverse();
  }

  // for each hash, get the file content
  for (const log of logs) {
    logger.debug(
      `[GIT.V] Pulling file content for hash ${log.hash} (${log.date}): ${options.filePath}`
    );
    try {
      const fileContent = await git.show([`${log.hash}:${options.filePath}`]);
      yield { commitHash: log.hash, date: new Date(log.date), fileContent };
    } catch (e) {
      logger.error(`[GIT.V] Could not get file content for hash ${log.hash}`);
      console.log(e);
      if (options.throwOnError) {
        throw e;
      }
    }
  }
}
