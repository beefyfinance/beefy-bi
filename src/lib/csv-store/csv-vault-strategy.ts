import * as fs from "fs";
import * as path from "path";
import { Chain } from "../../types/chain";
import { DATA_DIRECTORY } from "../../utils/config";
import { normalizeAddress } from "../../utils/ethers";
import { logger } from "../../utils/logger";
import glob from "glob";
import { CsvStore } from "../../utils/csv-store";

export interface BeefyVaultV6StrategiesData {
  blockNumber: number;
  datetime: Date;
  implementation: string;
}

class VaultStrategyCsvStore extends CsvStore<BeefyVaultV6StrategiesData, [Chain, string]> {
  async *getAllStrategyAddresses(chain: Chain) {
    const filePaths = await new Promise<string[]>((resolve, reject) => {
      const globPath = path.join(DATA_DIRECTORY, "chain", chain, "contracts", "*", "BeefyVaultV6", "strategies.csv");
      // options is optional
      glob(globPath, function (er, filePaths) {
        if (er) {
          return reject(er);
        }
        resolve(filePaths);
      });
    });

    for (const filePath of filePaths) {
      const parts = filePath.split("/");
      const contractAddress = parts[parts.length - 3];
      const rows = this.getReadIterator(chain, contractAddress);
      for await (const record of rows) {
        yield {
          ...record,
          // I forgot to normalize on write
          implementation: normalizeAddress(record.implementation),
        };
      }
    }
  }

  async deleteAllVaultStrategiesData(chain: Chain, vaultContractAddress: string, dryrun: boolean) {
    const rows = this.getReadIterator(chain, vaultContractAddress);
    logger.verbose(
      `[VAULT.S.STORE]${dryrun ? "[dryrun]" : ""} Deleting all strategy data for ${chain}:${vaultContractAddress}`
    );
    for await (const strategy of rows) {
      const contractDirectory = path.join(
        DATA_DIRECTORY,
        "chain",
        chain,
        "contracts",
        normalizeAddress(strategy.implementation)
      );

      logger.verbose(
        `[VAULT.S.STORE]${dryrun ? "[dryrun]" : ""} Deleting all strategy data for ${chain}:${vaultContractAddress}:${
          strategy.implementation
        }`
      );
      if (!dryrun) {
        await fs.promises.rmdir(contractDirectory, { recursive: true });
      }
    }
  }
}

export const vaultStrategyStore = new VaultStrategyCsvStore({
  loggerScope: "VaultStrategyStore",
  getFilePath: (chain: Chain, contractAddress: string) =>
    path.join(
      DATA_DIRECTORY,
      "chain",
      chain,
      "contracts",
      normalizeAddress(contractAddress),
      "BeefyVaultV6",
      "strategies.csv"
    ),
  csvColumns: [
    { name: "blockNumber", type: "integer" },
    { name: "datetime", type: "date" },
    { name: "implementation", type: "string" },
  ],
});
