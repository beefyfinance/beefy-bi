import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { normalizeAddress } from "../utils/ethers";
import { allSamplingPeriods, SamplingPeriod, samplingPeriodMs } from "../types/sampling";
import { sleep } from "../utils/async";
import { fetchBeefyVaultList, fetchContractCreationInfos } from "../lib/fetch-if-not-found-locally";
import { BeefyVaultV6PPFSData, ppfsStore } from "../lib/csv-vault-ppfs";
import { batchAsyncStream } from "../utils/batch";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";
import { shuffle } from "lodash";
import { runMain } from "../utils/process";
import { LOG_LEVEL, RPC_BACH_CALL_COUNT } from "../utils/config";
import { BeefyVault } from "../lib/git-get-all-vaults";
import { blockSamplesStore } from "../lib/csv-block-samples";
import { fetchBeefyPPFS } from "../lib/beefy/ppfs";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: {
        choices: ["all"].concat(allChainIds),
        alias: "c",
        demand: false,
        default: "all",
      },
      period: { choices: allSamplingPeriods, alias: "p", default: "4hour" },
      vaultId: { alias: "v", demand: false, string: true },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? shuffle(allChainIds) : [chain];
  const samplingPeriod = argv.period as SamplingPeriod;
  const vaultId = argv.vaultId || null;

  const chainPromises = chains.map(async (chain) => {
    try {
      await importChain(chain, samplingPeriod, vaultId);
    } catch (error) {
      logger.error(`[PPFS] Error importing ${chain} ppfs: ${error}`);
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  });
  await Promise.allSettled(chainPromises);

  logger.info(`[PPFS] Finished importing ppfs for ${chains.join(", ")} with period ${samplingPeriod}. Sleeping a bit`);
  await sleep(samplingPeriodMs[samplingPeriod] * 3);
}

async function importChain(chain: Chain, samplingPeriod: SamplingPeriod, vaultId: string | null) {
  logger.info(`[PPFS] Importing ${chain} ppfs with period ${samplingPeriod}.`);
  // find out which vaults we need to parse
  const vaults = shuffle(await fetchBeefyVaultList(chain));
  for (const vault of vaults) {
    if (vaultId && vault.id !== vaultId) {
      logger.debug(`[PPFS] Skipping vault ${vault.id}`);
      continue;
    }

    try {
      await importVault(chain, samplingPeriod, vault);
    } catch (e) {
      if (e instanceof ArchiveNodeNeededError) {
        logger.error(`[PPFS] Archive node needed, skipping vault ${chain}:${vault.id}`);
        continue;
      } else {
        logger.error(`[PPFS] Error fetching ppfs, skipping vault ${chain}:${vault.id}: ${JSON.stringify(e)}`);
        console.log(e);
        continue;
      }
    }
  }
  logger.info(`[PPFS] Finished importing ppfs for ${chain}. Sleeping for a bit`);
}

async function importVault(chain: Chain, samplingPeriod: SamplingPeriod, vault: BeefyVault) {
  logger.info(`[PPFS] Importing ppfs for ${chain}:${vault.id}`);

  const contractAddress = normalizeAddress(vault.token_address);

  // find out the vault creation block or last imported ppfs
  let lastImportedBlock = (await ppfsStore.getLastRow(chain, contractAddress, samplingPeriod))?.blockNumber || null;
  if (lastImportedBlock === null) {
    // get creation block of the contract
    const { blockNumber } = await fetchContractCreationInfos(chain, contractAddress);
    // we skip the creation block
    lastImportedBlock = blockNumber;
  }
  logger.debug(`[PPFS] importing from block ${lastImportedBlock} for ${chain}:${vault.id}`);
  const blockSampleStream = blockSamplesStore.getReadIteratorFrom(
    ({ blockNumber }) => !lastImportedBlock || blockNumber >= lastImportedBlock,
    chain,
    samplingPeriod
  );

  const writer = await ppfsStore.getWriter(chain, contractAddress, samplingPeriod);

  try {
    let batchSize = RPC_BACH_CALL_COUNT[chain];
    if (batchSize === "no-batching") {
      batchSize = 1;
    }
    for await (const blockDataBatch of batchAsyncStream(blockSampleStream, batchSize)) {
      logger.verbose(
        `[PPFS] Fetching data of ${chain}:${vault.id} (${contractAddress}) for ${blockDataBatch.length} blocks starting from ${blockDataBatch[0].blockNumber}`
      );
      const ppfss = await fetchBeefyPPFS(
        chain,
        contractAddress,
        blockDataBatch.map((blockData) => blockData.blockNumber)
      );
      const vaultData: BeefyVaultV6PPFSData[] = Array.from(ppfss.entries()).map(([idx, ppfs]) => ({
        blockNumber: blockDataBatch[idx].blockNumber,
        datetime: blockDataBatch[idx].datetime,
        pricePerFullShare: ppfs.toString(),
      }));

      await writer.writeBatch(vaultData);
    }
  } finally {
    await writer.close();
  }
}

runMain(main);
