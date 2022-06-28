import {
  getLastImportedOraclePrice,
  getOraclePriceWriteStream,
  OraclePriceData,
} from "../lib/csv-oracle-price";
import {
  BeefyVault,
  fetchBeefyVaultList,
} from "../lib/fetch-if-not-found-locally";
import { streamBeefyPrices } from "../lib/streamBeefyPrices";
import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { shuffle } from "lodash";
import { runMain } from "../utils/process";
import { allSamplingPeriods, SamplingPeriod } from "../lib/csv-block-samples";
import { batchAsyncStream } from "../utils/batch";

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
    logger.error(
      `Sampling period ${samplingPeriod} is not supported. Sleeping 4h.`
    );
    return sleep(1000 * 60 * 60 * 4);
  }

  for (const chain of chains) {
    try {
      logger.info(`[PRICES] Importing ${chain} prices`);
      await importChain(chain, samplingPeriod, oracleId);
      logger.info(`[PRICES] DONE importing ${chain} prices`);
    } catch (error) {
      logger.error(
        `[PRICES] Error importing ${chain} price: ${JSON.stringify(error)}`
      );
    }
  }

  logger.info(`[PRICES] Done importing prices, sleeping 4h`);
  return sleep(1000 * 60 * 60 * 4);
}

async function importChain(
  chain: Chain,
  samplingPeriod: SamplingPeriod,
  oracleId: string | null
) {
  const vaults = shuffle(await fetchBeefyVaultList(chain));
  for (const vault of vaults) {
    try {
      logger.verbose(`[PRICES] Importing ${chain}:${vault.id} prices`);
      await importVault(chain, samplingPeriod, vault, oracleId);
      logger.verbose(`[PRICES] DONE importing ${chain}:${vault.id} prices`);
    } catch (error) {
      logger.error(
        `[PRICES] Error importing ${chain}:${vault.id} price: ${JSON.stringify(
          error
        )}`
      );
    }
  }
}
async function importVault(
  chain: Chain,
  samplingPeriod: SamplingPeriod,
  vault: BeefyVault,
  onlyOracleId: string | null
) {
  const allOracleIds = [vault.price_oracle.want_oracleId].concat(
    shuffle(vault.price_oracle.assets)
  );

  for (const oracleId of allOracleIds) {
    if (onlyOracleId && oracleId !== onlyOracleId) {
      logger.debug(
        `[PRICES] Skipping oracle id ${chain}:${vault.id}:${oracleId}`
      );
      continue;
    }
    try {
      logger.verbose(
        `[PRICES] Importing ${chain}:${vault.id}:${oracleId} prices`
      );
      await importOracleId(chain, samplingPeriod, oracleId);
      logger.verbose(
        `[PRICES] DONE importing ${chain}:${vault.id}:${oracleId} prices`
      );
    } catch (error) {
      logger.error(
        `[PRICES] Error importing ${chain}:${
          vault.id
        }:${oracleId} price: ${JSON.stringify(error)}`
      );
    }
  }
}

async function importOracleId(
  chain: Chain,
  samplingPeriod: SamplingPeriod,
  oracleId: string
) {
  const lastPriceRow = await getLastImportedOraclePrice(
    oracleId,
    samplingPeriod
  );

  // if we don't have any local data, do a full fetch
  // if not, only fetch from last data point
  let readStream: ReturnType<typeof streamBeefyPrices>;
  if (lastPriceRow) {
    logger.debug(
      `[PRICES] Last price found for ${chain}:${oracleId}, importing from there: ${JSON.stringify(
        lastPriceRow
      )}`
    );
    readStream = streamBeefyPrices(chain, samplingPeriod, oracleId, {
      startDate: new Date(lastPriceRow.datetime.getTime() + 1000),
    });
  } else {
    logger.debug(
      `[PRICES] Last price NOT found for ${chain}:${oracleId}, importing all data`
    );
    readStream = streamBeefyPrices(chain, samplingPeriod, oracleId);
  }

  const writeStream = await getOraclePriceWriteStream(oracleId, samplingPeriod);
  try {
    for await (const priceBatch of batchAsyncStream(readStream, 100)) {
      logger.debug(
        `[PRICES] Writing batch of prices for ${chain}:${oracleId} for ${
          priceBatch.length
        } prices starting from ${priceBatch[0].datetime.toISOString()}`
      );
      writeStream.writeBatch(
        priceBatch.map((p) => ({
          datetime: p.datetime,
          usdValue: p.value,
        }))
      );
    }
  } finally {
    await writeStream.close();
  }
}

runMain(main);
