import BeefyVaultV6Abi from "../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { allChainIds, Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import { sleep } from "../utils/async";
import { streamBifiVaultUpgradeStratEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { LOG_LEVEL, shouldUseExplorer } from "../utils/config";
import { vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { shuffle } from "lodash";
import { ArchiveNodeNeededError, callLockProtectedRpc } from "../lib/shared-resources/shared-rpc";
import { ethers } from "ethers";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { streamBifiVaultUpgradeStratEventsFromRpc } from "../lib/streamContractEventsFromRpc";
import { BeefyVault } from "../types/beefy";
import { vaultListStore } from "../lib/beefy/vault-list";
import { contractCreationStore, contractLastTrxStore } from "../lib/json-store/contract-first-last-blocks";

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

  const chainPromises = chains.map(async (chain) => {
    try {
      await importChain(chain, vaultId);
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

async function importChain(chain: Chain, vaultId: string | null) {
  logger.info(`[STRATS] Importing ${chain} vault strategies...`);
  // find out which vaults we need to parse
  const vaults = shuffle(await vaultListStore.fetchData(chain));

  // for each vault, find out the creation date or last imported transfer
  for (const vault of vaults) {
    if (vaultId && vault.id !== vaultId) {
      logger.debug(`[STRATS] Skipping vault ${vault.id}`);
      continue;
    }

    try {
      await importVault(chain, vault);
    } catch (error) {
      if (error instanceof ArchiveNodeNeededError) {
        logger.error(`[STRATS] Archive node needed, skipping vault ${chain}:${vault.id}`);
      } else {
        logger.error(`[STRATS] Error importing ${chain}:${vault.id} strategies: ${JSON.stringify(error)}`);
      }
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  }

  logger.info(`[STRATS] Done importing strategies for ${chain}`);
}

async function importVault(chain: Chain, vault: BeefyVault) {
  const contractAddress = normalizeAddress(vault.token_address);
  logger.info(`[STRATS] Processing ${chain}:${vault.id} (${contractAddress})`);

  const lastImported = await vaultStrategyStore.getLastRow(chain, contractAddress);
  let startBlock: number | null = lastImported?.blockNumber || null;
  if (startBlock === null) {
    logger.debug(`[STRATS] No local data for ${chain}:${vault.id}, fetching contract creation info`);

    const { blockNumber } = await contractCreationStore.fetchData(chain, contractAddress);
    startBlock = blockNumber;
  } else {
    logger.debug(
      `[STRATS] Found local data for ${chain}:${vault.id}, fetching data starting from block ${startBlock + 1}`
    );
    startBlock = startBlock + 1;

    // add a shortcut hitting the rpc (it's faster)
    const latestStrat = await callLockProtectedRpc(chain, async (provider) => {
      const contract = new ethers.Contract(contractAddress, BeefyVaultV6Abi, provider);
      const [stragegy] = await contract.functions.strategy();
      return normalizeAddress(stragegy);
    });
    if (latestStrat === lastImported?.implementation) {
      logger.info(`[STRATS] ${chain}:${vault.id} already up to date, skipping`);
      return;
    }
  }

  const endBlock = (await contractLastTrxStore.fetchData(chain, contractAddress)).blockNumber;

  if (startBlock >= endBlock) {
    logger.info(`[STRATS] All data imported for ${contractAddress}`);
    return;
  }

  logger.info(`[STRATS] Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`);
  const writer = await vaultStrategyStore.getWriter(chain, contractAddress);

  const useExplorer = shouldUseExplorer(chain);

  try {
    if (useExplorer) {
      // Fuse and metis explorer requires an end block to be set
      // Error calling explorer https://explorer.fuse.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      // Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      const stopBlock = ["fuse", "metis"].includes(chain) ? endBlock : null;
      const stream = streamBifiVaultUpgradeStratEventsFromExplorer(chain, contractAddress, startBlock, stopBlock);
      for await (const eventBatch of batchAsyncStream(stream, 1000)) {
        logger.debug("[STRATS] Writing batch");
        await writer.writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            implementation: event.data.implementation,
          }))
        );
      }
    } else {
      const stream = streamBifiVaultUpgradeStratEventsFromRpc(chain, contractAddress);
      for await (const eventBatch of batchAsyncStream(stream, 100)) {
        logger.debug("[STRATS] Writing batch");
        await writer.writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            implementation: event.data.implementation,
          }))
        );
      }
    }
  } finally {
    await writer.close();
  }
}

runMain(main);
