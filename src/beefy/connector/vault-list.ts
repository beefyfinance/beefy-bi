import { Chain } from "../../types/chain";
import * as path from "path";
import { simpleGit, SimpleGit, SimpleGitOptions } from "simple-git";
import { GITHUB_RO_AUTH_TOKEN, GIT_WORK_DIRECTORY } from "../../utils/config";
import { sortBy } from "lodash";
import { normalizeAddress } from "../../utils/ethers";
import { getChainWNativeTokenAddress } from "../../utils/addressbook";
import { callLockProtectedGitRepo } from "../../lib/shared-resources/shared-gitrepo";
import { BeefyVault } from "../../types/beefy";
import { fileOrDirExists } from "../../utils/fs";
import prettier from "prettier";
import { rootLogger } from "../../utils/logger2";

const logger = rootLogger.child({ module: "beefy", component: "vault-list" });

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

export async function getAllVaultsFromGitHistory(chain: Chain): Promise<BeefyVault[]> {
  const vaultsByAddress: Record<string, RawBeefyVault> = {};

  logger.debug({ msg: "Fetching vault list from beefy-v2 repo git history", data: { chain } });

  const fileContentStreamV2 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-v2.git`
      : "git@github.com:beefyfinance/beefy-v2.git",
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
        logger.error({ msg: "Could not find vault earned token address for v2 vault", data: { vaultId: vault.id } });
        logger.trace(vault);
        continue;
      }
      const earnedTokenAddress = normalizeAddress(vault.earnedTokenAddress);
      if (!vaultsByAddress[earnedTokenAddress]) {
        vaultsByAddress[earnedTokenAddress] = vault;
      }
    }
  }

  // then v1 vaults
  logger.debug({ msg: "Fetching vault list from beefy-v1 repo git history", data: { chain } });
  const v1Chain = chain === "avax" ? "avalanche" : chain;
  const fileContentStreamV1 = gitStreamFileVersions({
    remote: GITHUB_RO_AUTH_TOKEN
      ? `https://${GITHUB_RO_AUTH_TOKEN}@github.com/beefyfinance/beefy-app.git`
      : "git@github.com:beefyfinance/beefy-app.git",
    branch: "prod",
    filePath: `src/features/configure/vault/${v1Chain}_pools.js`,
    workdir: path.join(GIT_WORK_DIRECTORY, "beefy-v1"),
    order: "recent-to-old",
    throwOnError: false,
  });
  // for v1, we only get the last of each month to make it quicker
  const includedMonths: Record<string, true> = {};

  for await (const fileVersion of fileContentStreamV1) {
    const month = fileVersion.date.toISOString().slice(0, 7);
    if (includedMonths[month]) {
      logger.debug({
        msg: "Skipping v1 vault for hash as we already imported one for this month",
        data: { commitHash: fileVersion.commitHash, month },
      });
      continue;
    }
    includedMonths[month] = true;
    // using prettier to transform js objects into proper json was the easiest way for me
    try {
      // remove js code to make json5-like string
      const jsonCode = "[" + fileVersion.fileContent.trim().split("\n").slice(1, -1).join("\n") + "]";

      // format json with comments in a standard way to make comment removing easy
      const json5Content = prettier.format(jsonCode, {
        semi: false,
        parser: "json5",
        quoteProps: "consistent",
        singleQuote: false,
      });

      const jsonContent = prettier.format(
        json5Content
          .replace(/\/\*(\s|\S)*?\*\//gm, "") // remove multiline comments
          .replace(/\/\/( .*\n|\n)/gm, ""), // remove single line comments
        { parser: "json", semi: false }
      );

      const vaults: RawBeefyVault[] = JSON.parse(jsonContent);

      for (const vault of vaults) {
        if (!vault.earnedTokenAddress) {
          logger.error({ msg: "Could not find vault earned token address for v1 vault", data: { vaultId: vault.id } });
          logger.trace(vault);
          continue;
        }
        const earnedTokenAddress = normalizeAddress(vault.earnedTokenAddress);
        if (!vaultsByAddress[earnedTokenAddress]) {
          vaultsByAddress[earnedTokenAddress] = vault;
        }
      }
    } catch (error) {
      logger.error({
        msg: "Could not parse vault list for v1 hash",
        data: { chain, commitHash: fileVersion.commitHash, date: fileVersion.date },
        error,
      });
      logger.debug(error);
    }
  }

  logger.debug({ msg: "All raw vaults found for chain, mapping to expected format", data: { chain } });
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
      logger.error({ msg: "Could not map raw vault to expected format", data: { rawVault }, error });
      logger.debug(error);
      throw error;
    }
  });
  logger.info({ msg: "Fetched vault configs", data: { total: vaults.length, chain } });
  return vaults;
}

async function* gitStreamFileVersions(options: {
  remote: string;
  workdir: string;
  branch: string;
  filePath: string;
  order: "recent-to-old" | "old-to-recent";
  throwOnError: boolean;
}): AsyncGenerator<{ commitHash: string; date: Date; fileContent: string }> {
  const baseOptions: Partial<SimpleGitOptions> = {
    binary: "git",
    maxConcurrentProcesses: 1,
  };

  // we can't make concurrent pulls
  await callLockProtectedGitRepo(options.workdir, async () => {
    // pull latest changes from remote or just clone remote
    if (!(await fileOrDirExists(options.workdir))) {
      logger.debug({ msg: "cloning remote locally", data: { remote: options.remote, workdir: options.workdir } });
      const git: SimpleGit = simpleGit({
        ...baseOptions,
        baseDir: GIT_WORK_DIRECTORY,
      });
      await git.clone(options.remote, options.workdir);
    } else {
      logger.debug({ msg: "Local repo found", data: { remote: options.remote, workdir: options.workdir } });
    }
  });

  // get a new git instance for the current git repo
  const git = simpleGit({
    ...baseOptions,
    baseDir: options.workdir,
  });
  // switch to the target branch
  await callLockProtectedGitRepo(options.workdir, async () => {
    logger.debug({ msg: "Changing branch to", data: { branch: options.branch } });
    await git.checkout(options.branch);
    logger.debug({ msg: "Pulling changes", data: { branch: options.branch } });
    await git.pull("origin", options.branch);
  });

  if (!(await fileOrDirExists(path.join(options.workdir, options.filePath)))) {
    logger.debug({ msg: "No file found", data: { filePath: options.filePath, branch: options.branch } });
    return;
  }

  // get all commit hashes for the target file
  logger.debug({
    msg: "Pulling all commit hashes for file",
    data: { filePath: options.filePath, branch: options.branch },
  });
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
    logger.debug({ msg: "Pulling file content", data: { hash: log.hash, date: log.date, filePath: options.filePath } });
    try {
      const fileContent = await git.show([`${log.hash}:${options.filePath}`]);
      yield { commitHash: log.hash, date: new Date(log.date), fileContent };
    } catch (error) {
      if (error instanceof Error && error.message.includes("exists on disk, but not in")) {
        logger.debug({
          msg: "File not found in commit, most likely the file was renamed",
          data: { hash: log.hash, filePath: options.filePath },
        });
      } else {
        logger.error({
          msg: "Could not get file content",
          data: { hash: log.hash, filePath: options.filePath },
          error,
        });
        logger.trace(error);
      }
      if (options.throwOnError) {
        throw error;
      }
    }
  }
}
