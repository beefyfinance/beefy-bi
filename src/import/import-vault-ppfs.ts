import { Chain } from "../types/chain";
import { logger } from "../utils/logger";
import { normalizeAddress } from "../utils/ethers";
import { allSamplingPeriods, SamplingPeriod, samplingPeriodMs } from "../types/sampling";
import { sleep } from "../utils/async";
import { BeefyVaultV6PPFSData, ppfsStore } from "../lib/csv-store/csv-vault-ppfs";
import { batchAsyncStream } from "../utils/batch";
import { runMain } from "../utils/process";
import { RPC_BACH_CALL_COUNT } from "../utils/config";
import { BeefyVault } from "../types/beefy";
import { blockSamplesStore } from "../lib/csv-store/csv-block-samples";
import { fetchBeefyPPFS } from "../beefy/connector/ppfs";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";

const main = foreachVaultCmd({
  loggerScope: "PPFS",
  additionalOptions: {
    period: { choices: allSamplingPeriods, alias: "p", default: "4hour" },
  },
  work: (argv, chain, vault) => importVault(chain, argv.period as SamplingPeriod, vault),
  onFinish: async (argv) => {
    const samplingPeriod = argv.period as SamplingPeriod;
    logger.info(`[PPFS] Finished importing ppfs with period ${samplingPeriod}. Sleeping a bit`);
    await sleep(samplingPeriodMs[samplingPeriod] * 3);
  },
  shuffle: true,
  parallelize: true,
});

async function importVault(chain: Chain, samplingPeriod: SamplingPeriod, vault: BeefyVault) {
  logger.info(`[PPFS] Importing ppfs for ${chain}:${vault.id}`);

  const contractAddress = normalizeAddress(vault.token_address);

  // find out the vault creation block or last imported ppfs
  let lastImportedBlock = (await ppfsStore.getLastRow(chain, contractAddress, samplingPeriod))?.blockNumber || null;
  if (lastImportedBlock === null) {
    // get creation block of the contract
    const { blockNumber } = await contractCreationStore.fetchData(chain, contractAddress);
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
