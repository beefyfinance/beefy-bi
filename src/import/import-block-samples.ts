import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { BlockDateInfos, fetchBlockData } from "../utils/ethers";
import {
  allSamplingPeriods,
  getBlockSamplesStorageWriteStream,
  getLastImportedSampleBlockData,
  SamplingPeriod,
  samplingPeriodMs,
} from "../lib/csv-block-samples";
import { LOG_LEVEL, MS_PER_BLOCK_ESTIMATE } from "../utils/config";
import { sleep } from "../utils/async";
import * as lodash from "lodash";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      period: { choices: allSamplingPeriods, alias: "p", default: "4hour" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const samplingPeriod = argv.period as SamplingPeriod;

  const chainsToImport = chain === "all" ? allChainIds : [chain];
  const allPromises = lodash.shuffle(chainsToImport).map(async (chain) => {
    try {
      await importChainBlockSamples(chain, samplingPeriod);
    } catch (e) {
      logger.error(
        `[BLOCKS] Error importing ${chain} block samples. Skipping. ${e}`
      );
      if (LOG_LEVEL === "trace") {
        console.log(e);
      }
    }
  });
  await Promise.allSettled(allPromises);

  logger.info(
    `[BLOCKS] Done importing ${chain} block samples with period ${samplingPeriod}. Sleeping for 10 * ${samplingPeriod}`
  );
  const ms = samplingPeriodMs[samplingPeriod];
  await sleep(10 * ms);
}

async function importChainBlockSamples(
  chain: Chain,
  samplingPeriod: SamplingPeriod
) {
  logger.info(
    `[BLOCKS] Importing ${chain} block samples with period ${samplingPeriod}.`
  );

  const { writeBatch } = await getBlockSamplesStorageWriteStream(
    chain,
    samplingPeriod
  );

  let lastImported = await getLastImportedSampleBlockData(
    chain,
    samplingPeriod
  );
  if (lastImported === null) {
    let firstBlock = await getFirstBlock(chain);
    // add special case for aurora to speed things up
    // all blocks before that are set to timestamp 0
    if (chain === "aurora") {
      firstBlock = await fetchBlockData(chain, 9820889);
    }
    await writeBatch([firstBlock]);
    lastImported = firstBlock;
  }

  const ms = samplingPeriodMs[samplingPeriod];
  const latestBlock = await fetchBlockData(chain, "latest");

  let blockCountToFill = Math.round(
    samplingPeriodMs[samplingPeriod] / MS_PER_BLOCK_ESTIMATE[chain]
  );
  while (
    latestBlock.datetime.getTime() - lastImported.datetime.getTime() >
    ms
  ) {
    const upperBound = await fetchBlockData(
      chain,
      Math.min(
        lastImported.blockNumber + blockCountToFill,
        latestBlock.blockNumber
      )
    );
    logger.verbose(
      `[BLOCKS] Importing blocks between ${lastImported.blockNumber} and ${upperBound.blockNumber}`
    );
    const innerBlocks = await fillBlockGaps(
      chain,
      samplingPeriod,
      lastImported,
      upperBound
    );
    if (lastImported.datetime.getTime() !== upperBound.datetime.getTime()) {
      await writeBatch([...innerBlocks, upperBound]);
    }
    lastImported = upperBound as BlockDateInfos;

    // adjust blockCountToFill
    if (innerBlocks.length < 80) {
      const multiplier =
        innerBlocks.length < 10 ? 4 : innerBlocks.length < 30 ? 2 : 1.5;
      const newBlockCountToFill = Math.round(blockCountToFill * multiplier);
      logger.debug(
        `[BLOCKS] Too few blocks imported (${innerBlocks.length}), increasing blockCountToFill (${blockCountToFill} -> ${newBlockCountToFill})`
      );
      blockCountToFill = newBlockCountToFill;
    } else if (innerBlocks.length > 120) {
      const multiplier =
        innerBlocks.length > 200 ? 0.5 : innerBlocks.length > 150 ? 0.65 : 0.8;
      const newBlockCountToFill = Math.round(blockCountToFill * multiplier);
      logger.debug(
        `[BLOCKS] Too many blocks imported (${innerBlocks.length}), decreasing blockCountToFill (${blockCountToFill} -> ${newBlockCountToFill})`
      );
      blockCountToFill = newBlockCountToFill;
    }

    // add a maximum block count to fill to 500 * period
    // because some blockchains have many useless blocks in the beginning (aurora)
    blockCountToFill = Math.min(
      Math.round((ms * 500) / MS_PER_BLOCK_ESTIMATE[chain]),
      blockCountToFill
    );
  }
}

async function fillBlockGaps(
  chain: Chain,
  samplingPeriod: SamplingPeriod,
  lowerBound: BlockDateInfos,
  upperBound: BlockDateInfos
): Promise<BlockDateInfos[]> {
  const ms = samplingPeriodMs[samplingPeriod];

  // sometimes the block takes too long and we can't get under the limit
  if (upperBound.blockNumber === lowerBound.blockNumber) {
    return [];
  }
  // sometimes there is no block in between
  if (upperBound.blockNumber === lowerBound.blockNumber + 1) {
    return [];
  }
  if (upperBound.datetime.getTime() - lowerBound.datetime.getTime() < ms) {
    return [];
  }
  // otherwise, pick the block number in the middle and fill gaps
  const midpointBlockNumber =
    lowerBound.blockNumber +
    Math.floor((upperBound.blockNumber - lowerBound.blockNumber) / 2);
  if (
    midpointBlockNumber <= lowerBound.blockNumber ||
    midpointBlockNumber >= upperBound.blockNumber
  ) {
    throw "NOPE";
  }
  const midPointBlockInfos = await fetchBlockData(chain, midpointBlockNumber);
  const beforeFill = await fillBlockGaps(
    chain,
    samplingPeriod,
    lowerBound,
    midPointBlockInfos
  );
  const afterFill = await fillBlockGaps(
    chain,
    samplingPeriod,
    midPointBlockInfos,
    upperBound
  );
  if (beforeFill.length <= 0 || afterFill.length <= 0) {
    return [...beforeFill, midPointBlockInfos, ...afterFill];
  }
  // maybe we should skip this block because it is too close to the next block
  const beforeBlock = beforeFill[beforeFill.length - 1];
  const afterBlock = afterFill[0];
  if (afterBlock.datetime.getTime() - beforeBlock.datetime.getTime() < ms) {
    return [...beforeFill, ...afterFill];
  } else {
    return [...beforeFill, midPointBlockInfos, ...afterFill];
  }
}

async function estimateMsPerBlock(chain: Chain) {
  const firstBlock = await getFirstBlock(chain);
  const latestBlock = await fetchBlockData(chain, "latest");
  console.log(firstBlock, latestBlock);
  const msPerBlockEstimate = Math.floor(
    (latestBlock.datetime.getTime() - firstBlock.datetime.getTime()) /
      (latestBlock.blockNumber - firstBlock.blockNumber)
  );
  return msPerBlockEstimate;
}

async function getFirstBlock(chain: Chain) {
  let firstBlock = await fetchBlockData(chain, 0);
  if (firstBlock.datetime.getTime() === 0) {
    firstBlock = await fetchBlockData(chain, 1);
  }
  return firstBlock;
}

main()
  .then(() => {
    logger.info("[BLOCKS] Done");
    process.exit(0);
  })
  .catch((e) => {
    console.log(e);
    logger.error(e);
    process.exit(1);
  });

process.on("SIGINT", async () => {
  logger.verbose(
    `[BLOCKS] SIGINT, waiting for write streams to close and exiting`
  );
  await sleep(1000);
  logger.info(`[BLOCKS] SIGINT, exiting`);
  process.exit(0);
});
