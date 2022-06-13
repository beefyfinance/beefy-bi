import {
  fetchBeefyVaultAddresses,
  fetchCachedContractLastTransaction,
  fetchContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import { allChainIds, Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import { sleep } from "../utils/async";
import { streamBifiVaultUpgradeStratEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { LOG_LEVEL } from "../utils/config";
import {
  getBeefyVaultV6StrategiesWriteStream,
  getLastImportedBeefyVaultV6StrategiesBlockNumber,
} from "../lib/csv-vault-strategy";

async function main() {
  const useExplorerFor: Chain[] = [
    "fantom",
    //"cronos", // explorer is buggy
    "bsc",
    "polygon",
    "heco",
    "avax",
    "moonbeam",
    "celo",
    "moonriver",
    "arbitrum",
    "aurora",
  ];
  for (const chain of allChainIds) {
    try {
      const source = useExplorerFor.includes(chain) ? "explorer" : "rpc";
      await importChain(chain, source);
    } catch (error) {
      logger.error(`[STRATS] Error importing ${chain} strategies: ${error}`);
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  }

  logger.info(`[STRATS] Done importing vault strategies, sleeping 24h`);
  await sleep(1000 * 60 * 60 * 24);
}

async function importChain(chain: Chain, source: "rpc" | "explorer") {
  logger.info(`[STRATS] Importing ${chain} vault strategies...`);
  // find out which vaults we need to parse
  const vaults = await fetchBeefyVaultAddresses(chain);

  // for each vault, find out the creation date or last imported transfer
  for (const vault of vaults) {
    const contractAddress = normalizeAddress(vault.token_address);
    logger.info(
      `[STRATS] Processing ${chain}:${vault.id} (${contractAddress})`
    );

    let startBlock = await getLastImportedBeefyVaultV6StrategiesBlockNumber(
      chain,
      contractAddress
    );
    if (startBlock === null) {
      logger.debug(
        `[STRATS] No local data for ${chain}:${vault.id}, fetching contract creation info`
      );

      const { blockNumber } = await fetchContractCreationInfos(
        chain,
        contractAddress
      );
      startBlock = blockNumber;
    } else {
      logger.debug(
        `[STRATS] Found local data for ${chain}:${
          vault.id
        }, fetching data starting from block ${startBlock + 1}`
      );
      startBlock = startBlock + 1;
    }

    const endBlock = (
      await fetchCachedContractLastTransaction(chain, contractAddress)
    ).blockNumber;

    if (startBlock >= endBlock) {
      logger.info(`[STRATS] All data imported for ${contractAddress}`);
      continue;
    }

    logger.info(
      `[STRATS] Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`
    );
    const { writeBatch } = await getBeefyVaultV6StrategiesWriteStream(
      chain,
      contractAddress
    );

    if (source === "explorer") {
      const stream = streamBifiVaultUpgradeStratEventsFromExplorer(
        chain,
        contractAddress,
        startBlock
      );
      for await (const eventBatch of batchAsyncStream(stream, 1000)) {
        logger.verbose("[STRATS] Writing batch");
        await writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            implementation: event.data.implementation,
          }))
        );
      }
    } else {
      const stream = streamBifiVaultUpgradeStratEventsFromExplorer(
        chain,
        contractAddress,
        startBlock
      );
      for await (const eventBatch of batchAsyncStream(stream, 100)) {
        logger.verbose("[STRATS] Writing batch");
        await writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            implementation: event.data.implementation,
          }))
        );
      }
    }
  }
}

main()
  .then(() => {
    logger.info("[STRATS] Done");
    process.exit(0);
  })
  .catch((e) => {
    console.log(e);
    logger.error(e);
    process.exit(1);
  });
