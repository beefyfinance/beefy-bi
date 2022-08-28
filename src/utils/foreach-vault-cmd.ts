import yargs from "yargs";
import { allChainIds, Chain } from "../types/chain";
import { BeefyVault } from "../types/beefy";
import { logger } from "../utils/logger";
import { vaultListStore } from "../beefy/connector/vault-list";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";
import { shuffle, sortBy } from "lodash";
import { LOG_LEVEL } from "./config";

const defaultYargsOptions = {
  chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
  vaultId: { type: "string", demand: false, alias: "v" },
};

type WorkArgv<O extends { [key: string]: yargs.Options }> = yargs.Argv<yargs.InferredOptionTypes<O>>["argv"];

export function foreachVaultCmd<O extends { [key: string]: yargs.Options }, TRes>(options: {
  loggerScope: string;
  additionalOptions: O;
  work: (argv: Awaited<WorkArgv<O>>, chain: Chain, vault: BeefyVault) => Promise<TRes>;
  onFinish?: (argv: Awaited<WorkArgv<O>>, result: { [chain in Chain]: TRes[] }) => Promise<void>;
  skipChain?: (chain: Chain) => boolean;
  shuffle: boolean;
  parallelize: boolean;
}) {
  return async function main() {
    const argv = await yargs(process.argv.slice(2))
      .usage("Usage: $0 [options]")
      .options({
        ...defaultYargsOptions,
        ...options.additionalOptions,
      }).argv;

    const chain = argv.chain as Chain | "all";
    let chains = chain === "all" ? allChainIds : [chain];
    const vaultId = argv.vaultId || null;

    async function processChain(chain: Chain) {
      logger.verbose(`[${options.loggerScope}] Processing chain ${chain}`);
      let vaults = await vaultListStore.fetchData(chain);
      if (options.shuffle) {
        vaults = shuffle(vaults);
      } else {
        vaults = sortBy(vaults, "id");
      }
      const results: TRes[] = [];
      for (const vault of vaults) {
        if (vaultId && vault.id !== vaultId) {
          logger.debug(`[${options.loggerScope}] Skipping vault ${vault.id}`);
          continue;
        }
        try {
          const res = await options.work(argv as any, chain, vault);
          results.push(res);
        } catch (e) {
          if (e instanceof ArchiveNodeNeededError) {
            logger.warn(`[${options.loggerScope}] Archive node needed, skipping vault ${chain}:${vault.id}`);
            continue;
          } else {
            logger.error(`[${options.loggerScope}] Error, skipping vault ${chain}:${vault.id}: ${e}`);
            if (LOG_LEVEL === "trace") {
              console.log(e);
            }
            continue;
          }
        }
      }

      logger.verbose(`[${options.loggerScope}] Finished processing chain ${chain}`);
      return results;
    }

    if (options.shuffle) {
      chains = shuffle(chains);
    } else {
      chains = sortBy(chains);
    }

    if (options.parallelize) {
      const chainPromises = chains.map(async (chain) => {
        try {
          return await processChain(chain);
        } catch (error) {
          logger.error(`[${options.loggerScope}] Error importing ${chain}: ${error}`);
          if (LOG_LEVEL === "trace") {
            console.log(error);
          }
        }
      });
      const results = await Promise.allSettled(chainPromises);

      if (options.onFinish) {
        const successes = {} as any as { [chain in Chain]: TRes[] };
        for (const [idx, res] of results.entries()) {
          if (res.status === "fulfilled" && res.value !== undefined) {
            successes[chains[idx]] = res.value;
          }
        }
        await options.onFinish(argv as any, successes);
      }
    } else {
      const successes = {} as any as { [chain in Chain]: TRes[] };
      for (const chain of chains) {
        if (options.skipChain && options.skipChain(chain)) {
          logger.info(`[${options.loggerScope}] Skipping ${chain}`);
          continue;
        }
        successes[chain] = await processChain(chain);
      }
      if (options.onFinish) {
        await options.onFinish(argv as any, successes);
      }
    }
  };
}
