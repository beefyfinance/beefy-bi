import * as fs from "fs";
import * as path from "path";
import { logger } from "../utils/logger";
import { runMain } from "../utils/process";
import { DATA_DIRECTORY, shouldUseExplorer, _forceUseSource } from "../utils/config";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { normalizeAddress } from "../utils/ethers";
import { deleteAllVaultData } from "../lib/csv-store/csv-vault";
import { BeefyVault } from "../types/beefy";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";
import { Chain } from "../types/chain";

const main = foreachVaultCmd({
  loggerScope: "CREATE.DATE.FIX",
  additionalOptions: {
    dryrun: { type: "boolean", demand: true, default: true, alias: "d" },
  },
  skipChain: (chain) => shouldUseExplorer(chain), // skip chains that have a decent explorer configured
  work: (argv, chain, vault) => fixVault(chain, vault, argv.dryrun),
  shuffle: false,
  parallelize: false,
});

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
  const localCreationInfos = await contractCreationStore.getLocalData(chain, contractAddress);
  if (!localCreationInfos) {
    logger.debug(`[CREATE.DATE.FIX] No local creation date for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  logger.verbose(
    `[CREATE.DATE.FIX] Local creation date for ${chain}:${vault.id}: ${localCreationInfos.datetime.toISOString()}`
  );

  const trueCreationInfos = await contractCreationStore.forceFetchData(chain, contractAddress);
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
