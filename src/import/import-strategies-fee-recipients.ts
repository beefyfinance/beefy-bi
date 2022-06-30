import { getBeefyStrategyFeeRecipients } from "../lib/fetch-if-not-found-locally";
import { allChainIds, Chain } from "../types/chain";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { getAllStrategyAddresses } from "../lib/csv-vault-strategy";
import { LOG_LEVEL } from "../utils/config";
import { runMain } from "../utils/process";
import { shuffle } from "lodash";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: ["all"].concat(allChainIds), alias: "c", demand: true },
      strategyAddress: { type: "string", alias: "s", demand: false },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const strategyAddress = argv.strategyAddress
    ? normalizeAddress(argv.strategyAddress)
    : null;

  logger.info(`[FEE.ADDR] Importing ${chain} ERC20 transfer events...`);
  const chains = chain === "all" ? shuffle(allChainIds) : [chain];

  // fetch all chains in parallel
  const chainPromises = chains.map(async (chain) => {
    try {
      await importChain(chain, strategyAddress);
    } catch (error) {
      logger.error(`[STRATS] Error importing ${chain} strategies: ${error}`);
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  });
  await Promise.allSettled(chainPromises);

  logger.info(
    `[FEE.ADDR] Done importing ${chain} fee recipients, sleeping 1 day (in case there is new vaults)`
  );
  await sleep(1000 * 60 * 60 * 24 * 10);
}

async function importChain(chain: Chain, strategyAddress: string | null) {
  try {
    const strategies = getAllStrategyAddresses(chain);
    for await (const strategy of strategies) {
      if (strategyAddress && strategy.implementation !== strategyAddress) {
        logger.debug(`[FEE.ADDR] Skipping strategy ${strategy.implementation}`);
        continue;
      }

      try {
        await getBeefyStrategyFeeRecipients(chain, strategy.implementation);
      } catch (e) {
        if (e instanceof ArchiveNodeNeededError) {
          logger.error(
            `[FEE.ADDR] Archive node needed, skipping vault ${chain}:${strategy.implementation}`
          );
        } else {
          logger.error(
            `[FEE.ADDR] Error importing fee recipients. Skipping ${chain}:${
              strategy.implementation
            }. ${JSON.stringify(e)}`
          );
        }
      }
    }
  } catch (e) {
    logger.error(
      `[FEE.ADDR] Error importing fee recipients. Skipping ${chain}. ${JSON.stringify(
        e
      )}`
    );
  }
  logger.info(`[FEE.ADDR] Done importing fee recipients for ${chain}`);
}

runMain(main);
