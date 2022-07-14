import {
  fetchBeefyVaultList,
  getLocalBeefyVaultList,
  getLocalContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import * as fs from "fs";
import * as path from "path";
import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { BeefyVault } from "../lib/git-get-all-vaults";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";
import {
  DATA_DIRECTORY,
  shouldUseExplorer,
  _forceUseSource,
} from "../utils/config";
import { getContractCreationInfosFromRPC } from "../lib/contract-transaction-infos";
import { deleteAllVaultStrategiesData } from "../lib/csv-vault-strategy";
import { normalizeAddress } from "../utils/ethers";
import { makeDataDirRecursive } from "../lib/make-data-dir-recursive";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: true, alias: "v" },
      forceSource: { choices: ["explorer", "rpc"], demand: false, alias: "s" },
      dryrun: { type: "boolean", demand: true, default: true, alias: "d" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId;
  const dryrun = argv.dryrun || true;
  const forceSource = (argv.forceSource || null) as "explorer" | "rpc" | null;

  if (forceSource) {
    _forceUseSource(forceSource);
  }

  logger.info(`[ERC20.T.FIX] Dryrun: ${dryrun}`);

  for (const chain of chains) {
    const vaults = await fetchBeefyVaultList(chain);
    for (const vault of vaults) {
      if (vault.id !== vaultId) {
        logger.debug(`[ERC20.T.FIX] Skipping vault ${vault.id}`);
        continue;
      }
      try {
        await fixVault(chain, vault, dryrun);
      } catch (e) {
        if (e instanceof ArchiveNodeNeededError) {
          logger.error(
            `[ERC20.T.FIX] Archive node needed, skipping vault ${chain}:${vault.id}`
          );
          continue;
        } else {
          logger.error(
            `[ERC20.T.FIX] Error fixing transfers, skipping vault ${chain}:${
              vault.id
            }: ${JSON.stringify(e)}`
          );
          console.log(e);
          continue;
        }
      }
    }
  }
}

//AEAZEAZEAZEAZE;
// for the actual target vault
// fetch the actual creation date from RPC using OwnershipTransfered logs
// if local creation date and RPC date are the same, do nothing
// otherwise
// write new creation date to local file
// find all strategies
// delete all files from all strategies
// delete all files for this vault

// manually purge db or full db reload

async function fixVault(chain: Chain, vault: BeefyVault, dryrun: boolean) {
  const contractAddress = vault.token_address;
  const localCreationInfos = await getLocalContractCreationInfos(
    chain,
    contractAddress
  );
  if (!localCreationInfos) {
    logger.debug(
      `[ERC20.T.FIX] No local creation date for ${chain}:${vault.id}. Skipping.`
    );
    return;
  }
  logger.verbose(
    `[ERC20.T.FIX] Local creation date for ${chain}:${
      vault.id
    }: ${localCreationInfos.datetime.toISOString()}`
  );
  const trueCreationInfos = await getContractCreationInfosFromRPC(
    chain,
    contractAddress,
    "4hour"
  );
  if (!trueCreationInfos) {
    logger.debug(
      `[ERC20.T.FIX] Could not find RPC creation date for ${chain}:${vault.id}. Skipping.`
    );
    return;
  }
  logger.verbose(
    `[ERC20.T.FIX] RPC creation date for ${chain}:${
      vault.id
    }: ${trueCreationInfos.datetime.toISOString()}`
  );

  if (
    trueCreationInfos.datetime.getTime() ===
    localCreationInfos.datetime.getTime()
  ) {
    logger.debug(
      `[ERC20.T.FIX] Creation dates are the same for ${chain}:${vault.id}. Skipping.`
    );
    return;
  }
  if (
    trueCreationInfos.datetime.getTime() > localCreationInfos.datetime.getTime()
  ) {
    logger.error(
      `[ERC20.T.FIX] RPC creation date is newer than local for ${chain}:${vault.id}. Skipping.`
    );
    return;
  }

  // delete strategy data
  await deleteAllVaultStrategiesData(chain, contractAddress, dryrun);

  // delete vault data
  logger.info(
    `[ERC20.T.FIX]${
      dryrun ? "[dryrun]" : ""
    } Deleting all vault data for ${chain}:${contractAddress}`
  );
  const contractDirectory = path.join(
    DATA_DIRECTORY,
    "chain",
    chain,
    "contracts",
    normalizeAddress(contractAddress)
  );

  logger.verbose(
    `[VAULT.S.STORE]${
      dryrun ? "[dryrun]" : ""
    } Deleting all strategy data for ${chain}:${contractAddress}`
  );
  if (!dryrun) {
    await fs.promises.rmdir(contractDirectory, { recursive: true });
  }

  // remake the directory for the vault
  await makeDataDirRecursive(
    path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(contractAddress)
    )
  );

  // write new creation date
  logger.info(
    `[ERC20.T.FIX] Writing new creation date for ${chain}:${
      vault.id
    }. Old date: ${localCreationInfos.datetime.toISOString()}, new date: ${trueCreationInfos.datetime.toISOString()}`
  );
  if (!dryrun) {
    const creationDatePath = path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(contractAddress),
      "creation_date.json"
    );
    await fs.promises.writeFile(
      creationDatePath,
      JSON.stringify(trueCreationInfos)
    );
  }
}

runMain(main);
