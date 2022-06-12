import { _fetchContractFirstLastTrx } from "../lib/contract-transaction-infos";
import { allChainIds, Chain } from "../types/chain";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { normalizeAddress } from "../utils/ethers";
import {
  allSamplingPeriods,
  SamplingPeriod,
  samplingPeriodMs,
  streamBlockSamplesFrom,
} from "../lib/csv-block-samples";
import { backOff } from "exponential-backoff";
import { sleep } from "../utils/async";
import {
  fetchBeefyVaultAddresses,
  fetchContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import {
  BeefyVaultV6PPFSData,
  fetchBeefyPPFS,
  getBeefyVaultV6PPFSWriteStream,
  getLastImportedBeefyVaultV6PPFSBlockNumber,
} from "../lib/csv-vault-ppfs";
import { batchAsyncStream } from "../utils/batch";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: allChainIds, alias: "c", demand: true },
      period: { choices: allSamplingPeriods, alias: "p", default: "15min" },
    }).argv;

  const chain = argv.chain as Chain;
  const samplingPeriod = argv.period as SamplingPeriod;

  logger.info(`[PPFS] Importing ${chain} ppfs with period ${samplingPeriod}.`);
  // find out which vaults we need to parse
  const vaults = await fetchBeefyVaultAddresses(chain);
  for (const vault of vaults) {
    logger.info(`[PPFS] Importing ppfs for ${chain}:${vault.id}`);

    const contractAddress = normalizeAddress(vault.token_address);

    // find out the vault creation block or last imported ppfs
    let lastImportedBlock = await getLastImportedBeefyVaultV6PPFSBlockNumber(
      chain,
      contractAddress,
      samplingPeriod
    );
    if (lastImportedBlock === null) {
      // get creation block of the contract
      const { blockNumber } = await fetchContractCreationInfos(
        chain,
        contractAddress
      );
      // we skip the creation block
      lastImportedBlock = blockNumber;
    }
    logger.debug(
      `[PPFS] importing from block ${lastImportedBlock} for ${chain}:${vault.id}`
    );
    const blockSampleStream = streamBlockSamplesFrom(
      chain,
      samplingPeriod,
      lastImportedBlock
    );

    const { writeBatch } = await getBeefyVaultV6PPFSWriteStream(
      chain,
      contractAddress,
      samplingPeriod
    );

    for await (const blockDataBatch of batchAsyncStream(
      blockSampleStream,
      10
    )) {
      logger.verbose(
        `[PPFS] Fetching data of ${chain}:${vault.id} (${contractAddress}) for ${blockDataBatch.length} blocks starting from ${blockDataBatch[0].blockNumber}`
      );
      const vaultData: BeefyVaultV6PPFSData[] = [];
      for (const blockData of blockDataBatch) {
        vaultData.push({
          blockNumber: blockData.blockNumber,
          pricePerFullShare: (
            await getBeefyPPFSAtBlockWithRetry(
              chain,
              contractAddress,
              blockData.blockNumber
            )
          ).toString(),
        });
      }
      writeBatch(vaultData);
    }
  }
  logger.info(
    `[PPFS] Finished importing ppfs for ${chain}. Sleeping for a bit`
  );
  await sleep(samplingPeriodMs[samplingPeriod] * 10);
}

// be nice to rpcs or you'll get banned
const minMsBetweenCalls = 1000;
let lastCall = new Date(0);
async function getBeefyPPFSAtBlockWithRetry(
  chain: Chain,
  contractAddress: string,
  blockNumber: number
) {
  return backOff(
    async () => {
      const now = new Date();
      if (now.getTime() - lastCall.getTime() < minMsBetweenCalls) {
        await sleep(minMsBetweenCalls - (now.getTime() - lastCall.getTime()));
      }
      const data = fetchBeefyPPFS(chain, contractAddress, blockNumber);
      lastCall = new Date();
      return data;
    },
    {
      retry: async (error, attemptNumber) => {
        logger.info(
          `[BLOCKS] Error on attempt ${attemptNumber} fetching block data of ${chain}:${blockNumber}: ${error}`
        );
        console.error(error);
        return true;
      },
      numOfAttempts: 10,
      startingDelay: 1000,
    }
  );
}

main()
  .then(() => {
    logger.info("Done");
    process.exit(0);
  })
  .catch((e) => {
    console.log(e);
    logger.error(e);
    process.exit(1);
  });
