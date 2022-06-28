import {
  BeefyVault,
  fetchBeefyVaultAddresses,
  fetchCachedContractLastTransaction,
  fetchContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { allChainIds, Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import { sleep } from "../utils/async";
import { streamBifiVaultUpgradeStratEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import {
  CHAINS_WITH_ETHSCAN_BASED_EXPLORERS,
  LOG_LEVEL,
} from "../utils/config";
import {
  getBeefyVaultV6StrategiesWriteStream,
  getLastImportedBeefyVaultV6Strategy,
} from "../lib/csv-vault-strategy";
import { shuffle } from "lodash";
import {
  ArchiveNodeNeededError,
  callLockProtectedRpc,
} from "../lib/shared-resources/shared-rpc";
import { ethers } from "ethers";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { streamBifiVaultUpgradeStratEventsFromRpc } from "../lib/streamContractEventsFromRpc";

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
      vaultId: { alias: "v", demand: false, string: true },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? shuffle(allChainIds) : [chain];
  const vaultId = argv.vaultId || null;

  const useExplorerFor: Chain[] = CHAINS_WITH_ETHSCAN_BASED_EXPLORERS;

  const chainPromises = chains.map(async (chain) => {
    const source = useExplorerFor.includes(chain) ? "explorer" : "rpc";
    try {
      const source = useExplorerFor.includes(chain) ? "explorer" : "rpc";
      await importChain(chain, source, vaultId);
    } catch (error) {
      logger.error(`[STRATS] Error importing ${chain} strategies: ${error}`);
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  });
  await Promise.allSettled(chainPromises);

  logger.info(`[STRATS] Done importing vault strategies, sleeping 24h`);
  await sleep(1000 * 60 * 60 * 24);
}

async function importChain(
  chain: Chain,
  source: "rpc" | "explorer",
  vaultId: string | null
) {
  logger.info(`[STRATS] Importing ${chain} vault strategies...`);
  // find out which vaults we need to parse
  const vaults = shuffle(await fetchBeefyVaultAddresses(chain));

  // for each vault, find out the creation date or last imported transfer
  for (const vault of vaults) {
    if (vaultId && vault.id !== vaultId) {
      logger.debug(`[STRATS] Skipping vault ${vault.id}`);
      continue;
    }

    try {
      await importVault(chain, source, vault);
    } catch (error) {
      if (error instanceof ArchiveNodeNeededError) {
        logger.error(
          `[STRATS] Archive node needed, skipping vault ${chain}:${vault.id}`
        );
      } else {
        logger.error(
          `[STRATS] Error importing ${chain}:${
            vault.id
          } strategies: ${JSON.stringify(error)}`
        );
      }
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  }
}

async function importVault(
  chain: Chain,
  source: "rpc" | "explorer",
  vault: BeefyVault
) {
  const contractAddress = normalizeAddress(vault.token_address);
  logger.info(`[STRATS] Processing ${chain}:${vault.id} (${contractAddress})`);

  const lastImported = await getLastImportedBeefyVaultV6Strategy(
    chain,
    contractAddress
  );
  let startBlock: number | null = lastImported?.blockNumber || null;
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

    // add a shortcut hitting the rpc (it's faster)
    const latestStrat = await callLockProtectedRpc(chain, async (provider) => {
      const contract = new ethers.Contract(
        contractAddress,
        BeefyVaultV6Abi,
        provider
      );
      const [stragegy] = await contract.functions.strategy();
      return normalizeAddress(stragegy);
    });
    if (latestStrat === lastImported?.implementation) {
      logger.info(`[STRATS] ${chain}:${vault.id} already up to date, skipping`);
      return;
    }
  }

  const endBlock = (
    await fetchCachedContractLastTransaction(chain, contractAddress)
  ).blockNumber;

  if (startBlock >= endBlock) {
    logger.info(`[STRATS] All data imported for ${contractAddress}`);
    return;
  }

  logger.info(
    `[STRATS] Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`
  );
  const { writeBatch } = await getBeefyVaultV6StrategiesWriteStream(
    chain,
    contractAddress
  );

  if (source === "explorer") {
    // Fuse and metis explorer requires an end block to be set
    // Error calling explorer https://explorer.fuse.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
    // Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
    const stopBlock = ["fuse", "metis"].includes(chain) ? endBlock : null;
    const stream = streamBifiVaultUpgradeStratEventsFromExplorer(
      chain,
      contractAddress,
      startBlock,
      stopBlock
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
    const stream = streamBifiVaultUpgradeStratEventsFromRpc(
      chain,
      contractAddress
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

runMain(main);
