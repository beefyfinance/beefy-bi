import { streamBeefyPrices } from "../lib/streamBeefyPrices";
import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { shuffle, uniq } from "lodash";
import { runMain } from "../utils/process";
import { allSamplingPeriods, SamplingPeriod } from "../types/sampling";
import { batchAsyncStream } from "../utils/batch";
import { BeefyVault } from "../types/beefy";
import { oraclePriceStore } from "../lib/csv-store/csv-oracle-price";
import { vaultListStore } from "../lib/beefy/vault-list";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: ["all"].concat(allChainIds), alias: "c", demand: true },
      samplingPeriod: {
        choices: allSamplingPeriods,
        alias: "s",
        demand: false,
        default: "15min",
      },
      oracleId: { type: "string", demand: false, alias: "i" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const samplingPeriod = argv.samplingPeriod as SamplingPeriod;
  const chains = chain === "all" ? shuffle(allChainIds) : [chain];
  const oracleId = argv.oracleId || null;

  if (samplingPeriod !== "15min") {
    logger.error(`[PRICES] Sampling period ${samplingPeriod} is not supported. Sleeping 4h.`);
    return sleep(1000 * 60 * 60 * 4);
  }

  const allOracleIds = new Set<string>();
  if (oracleId) {
    allOracleIds.add(oracleId);
  } else {
    for (const chain of chains) {
      const vaults = await vaultListStore.fetchData(chain);
      for (const vault of vaults) {
        const vaultOracleIds = [vault.price_oracle.want_oracleId].concat(shuffle(vault.price_oracle.assets));
        for (const vaultOracleId of vaultOracleIds) {
          allOracleIds.add(vaultOracleId);
        }
      }
    }
  }

  for (const oracleId of allOracleIds) {
    await importOracleId(samplingPeriod, oracleId);
  }

  logger.info(`[PRICES] Done importing prices, sleeping 4h`);
  return sleep(1000 * 60 * 60 * 4);
}

async function importOracleId(samplingPeriod: SamplingPeriod, oracleId: string) {
  const lastPriceRow = await oraclePriceStore.getLastRow(oracleId, samplingPeriod);

  // if we don't have any local data, do a full fetch
  // if not, only fetch from last data point
  let readStream: ReturnType<typeof streamBeefyPrices>;
  if (lastPriceRow) {
    logger.debug(`[PRICES] Last price found for ${oracleId}, importing from there: ${JSON.stringify(lastPriceRow)}`);
    readStream = streamBeefyPrices(samplingPeriod, oracleId, {
      startDate: new Date(lastPriceRow.datetime.getTime() + 1000),
    });
  } else {
    logger.debug(`[PRICES] Last price NOT found for ${oracleId}, importing all data`);
    readStream = streamBeefyPrices(samplingPeriod, oracleId);
  }

  const writer = await oraclePriceStore.getWriter(oracleId, samplingPeriod);
  try {
    for await (const priceBatch of batchAsyncStream(readStream, 100)) {
      logger.debug(
        `[PRICES] Writing batch of prices for ${oracleId} for ${
          priceBatch.length
        } prices starting from ${priceBatch[0].datetime.toISOString()}`
      );
      await writer.writeBatch(
        priceBatch.map((p) => ({
          datetime: p.datetime,
          usdValue: p.value,
        }))
      );
    }
  } finally {
    await writer.close();
  }
}

runMain(main);
