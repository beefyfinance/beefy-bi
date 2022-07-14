import * as fs from "fs";
import * as path from "path";
import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";
import { DATA_DIRECTORY, shouldUseExplorer, _forceUseSource } from "../utils/config";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { normalizeAddress } from "../utils/ethers";
import { deleteAllVaultData } from "../lib/csv-store/csv-vault";
import { vaultListStore } from "../lib/beefy/vault-list";
import { BeefyVault } from "../types/beefy";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: false, alias: "v" },
      dryrun: { type: "boolean", demand: true, default: true, alias: "d" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId || null;
  const dryrun = argv.dryrun || true;

  logger.info(`[CREATE.DATE.FIX] Dryrun: ${dryrun}`);

  for (const chain of chains) {
    const useExplorer = shouldUseExplorer(chain);
    if (useExplorer) {
      logger.info(`[BLOCKS] Skipping ${chain} because it has decent explorer`);
      continue;
    }
    const vaults = await vaultListStore.fetchData(chain);
    for (const vault of vaults) {
      if (vaultId && vault.id !== vaultId) {
        logger.debug(`[CREATE.DATE.FIX] Skipping vault ${vault.id}`);
        continue;
      }
      try {
        await fixVault(chain, vault, dryrun);
      } catch (e) {
        if (e instanceof ArchiveNodeNeededError) {
          logger.error(`[CREATE.DATE.FIX] Archive node needed, skipping vault ${chain}:${vault.id}`);
          continue;
        } else {
          logger.error(
            `[CREATE.DATE.FIX] Error fetching transfers, skipping vault ${chain}:${vault.id}: ${JSON.stringify(e)}`
          );
          console.log(e);
          continue;
        }
      }
    }
  }
}

// for each chain
// if explorer is decent, do nothing
// otherwise
// for each vault
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
  const localCreationInfos = await contractCreationStore.fetchData(chain, contractAddress);
  if (!localCreationInfos) {
    logger.debug(`[CREATE.DATE.FIX] No local creation date for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  logger.verbose(
    `[CREATE.DATE.FIX] Local creation date for ${chain}:${vault.id}: ${localCreationInfos.datetime.toISOString()}`
  );

  // make sure we use RPC
  _forceUseSource("rpc");

  const trueCreationInfos = await contractCreationStore.fetchData(chain, contractAddress);
  if (!trueCreationInfos) {
    logger.debug(`[CREATE.DATE.FIX] Could not find RPC creation date for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  logger.verbose(
    `[CREATE.DATE.FIX] RPC creation date for ${chain}:${vault.id}: ${trueCreationInfos.datetime.toISOString()}`
  );

  if (trueCreationInfos.datetime.getTime() === localCreationInfos.datetime.getTime()) {
    logger.debug(`[CREATE.DATE.FIX] Creation dates are the same for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  if (trueCreationInfos.datetime.getTime() > localCreationInfos.datetime.getTime()) {
    logger.error(`[CREATE.DATE.FIX] RPC creation date is newer than local for ${chain}:${vault.id}. Skipping.`);
    return;
  }

  await deleteAllVaultData(chain, contractAddress, dryrun);

  // write new creation date
  logger.info(
    `[CREATE.DATE.FIX] Writing new creation date for ${chain}:${
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
    await fs.promises.writeFile(creationDatePath, JSON.stringify(trueCreationInfos));
  }
}

runMain(main);
