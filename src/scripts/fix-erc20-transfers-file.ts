import * as fs from "fs";
import * as path from "path";
import { Chain } from "../types/chain";
import { logger } from "../utils/logger";
import { runMain } from "../utils/process";
import { DATA_DIRECTORY, _forceUseSource } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { deleteAllVaultData } from "../lib/csv-store/csv-vault";
import { BeefyVault } from "../types/beefy";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";

const main = foreachVaultCmd({
  loggerScope: "ERC20.T.FIX",
  additionalOptions: {
    forceSource: { choices: ["explorer", "rpc"], demand: false, alias: "s" },
    dryrun: { type: "boolean", demand: true, default: true, alias: "d" },
  },
  work: (argv, chain, vault) => {
    const forceSource = (argv.forceSource || null) as "explorer" | "rpc" | null;
    if (forceSource) {
      _forceUseSource(forceSource);
    }
    return fixVault(chain, vault, argv.dryrun);
  },
  shuffle: false,
  parallelize: false,
});

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
  const localCreationInfos = await contractCreationStore.fetchData(chain, contractAddress);
  if (!localCreationInfos) {
    logger.debug(`[ERC20.T.FIX] No local creation date for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  logger.verbose(
    `[ERC20.T.FIX] Local creation date for ${chain}:${vault.id}: ${localCreationInfos.datetime.toISOString()}`
  );

  // make sure we use RPC
  _forceUseSource("rpc");

  const trueCreationInfos = await contractCreationStore.fetchData(chain, contractAddress);

  if (!trueCreationInfos) {
    logger.debug(`[ERC20.T.FIX] Could not find RPC creation date for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  logger.verbose(
    `[ERC20.T.FIX] RPC creation date for ${chain}:${vault.id}: ${trueCreationInfos.datetime.toISOString()}`
  );

  if (trueCreationInfos.datetime.getTime() === localCreationInfos.datetime.getTime()) {
    logger.debug(`[ERC20.T.FIX] Creation dates are the same for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  if (trueCreationInfos.datetime.getTime() > localCreationInfos.datetime.getTime()) {
    logger.error(`[ERC20.T.FIX] RPC creation date is newer than local for ${chain}:${vault.id}. Skipping.`);
    return;
  }

  await deleteAllVaultData(chain, contractAddress, dryrun);

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
    await fs.promises.writeFile(creationDatePath, JSON.stringify(trueCreationInfos));
  }
}

runMain(main);
