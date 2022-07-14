import { Chain } from "../../types/chain";
import { DATA_DIRECTORY } from "../../utils/config";
import { logger } from "../../utils/logger";
import * as path from "path";
import * as fs from "fs";
import { normalizeAddress } from "../../utils/ethers";
import { vaultStrategyStore } from "./csv-vault-strategy";
import { makeDataDirRecursive } from "../../utils/make-data-dir-recursive";

export async function deleteAllVaultData(chain: Chain, contractAddress: string, dryrun: boolean) {
  // delete strategy data
  await vaultStrategyStore.deleteAllVaultStrategiesData(chain, contractAddress, dryrun);

  logger.info(`[VAULT.STORE]${dryrun ? "[dryrun]" : ""} Deleting all vault data for ${chain}:${contractAddress}`);
  const contractDirectory = path.join(DATA_DIRECTORY, "chain", chain, "contracts", normalizeAddress(contractAddress));

  logger.verbose(`[VAULT.STORE]${dryrun ? "[dryrun]" : ""} Deleting all strategy data for ${chain}:${contractAddress}`);
  if (!dryrun) {
    await fs.promises.rmdir(contractDirectory, { recursive: true });
  }

  // recreate the directory for the vault
  await makeDataDirRecursive(path.join(DATA_DIRECTORY, "chain", chain, "contracts", normalizeAddress(contractAddress)));
}
