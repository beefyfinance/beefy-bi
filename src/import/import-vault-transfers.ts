import {
  getERC20TransferStorageWriteStream,
  getLastImportedERC20TransferEvent,
} from "../lib/csv-transfer-events";
import {
  fetchBeefyVaultList,
  fetchCachedContractLastTransaction,
  fetchContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import { streamERC20TransferEventsFromRpc } from "../lib/streamContractEventsFromRpc";
import { allChainIds, Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { streamERC20TransferEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { shuffle } from "lodash";
import { runMain } from "../utils/process";
import { LOG_LEVEL, shouldUseExplorer } from "../utils/config";
import { BeefyVault } from "../lib/git-get-all-vaults";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";

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
      logger.error(`[PPFS] Error importing ${chain} ppfs: ${error}`);
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  });
  await Promise.allSettled(chainPromises);

  logger.info(
    `[ERC20.T] Done importing ${chain} ERC20 transfer events, sleeping 4h`
  );
  await sleep(1000 * 60 * 60 * 4);
}

async function importChain(chain: Chain, vaultId: string | null) {
  logger.info(`[ERC20.T] Importing ${chain} ERC20 transfer events...`);
  // find out which vaults we need to parse
  const vaults = shuffle(await fetchBeefyVaultList(chain));

  // for each vault, find out the creation date or last imported transfer
  for (const vault of vaults) {
    if (vaultId && vault.id !== vaultId) {
      logger.debug(`[ERC20.T] Skipping vault ${vault.id}`);
      continue;
    }

    try {
      await importVault(chain, vault);
    } catch (e) {
      if (e instanceof ArchiveNodeNeededError) {
        logger.error(
          `[ERC20.T] Archive node needed, skipping vault ${chain}:${vault.id}`
        );
        continue;
      } else {
        logger.error(
          `[ERC20.T] Error fetching transfers, skipping vault ${chain}:${
            vault.id
          }: ${JSON.stringify(e)}`
        );
        continue;
      }
    }
  }
  logger.info(`[ERC20.T] Done importing ${chain} ERC20 transfer events`);
}

async function importVault(chain: Chain, vault: BeefyVault) {
  const contractAddress = normalizeAddress(vault.token_address);
  logger.info(`[ERC20.T] Processing ${chain}:${vault.id} (${contractAddress})`);

  let startBlock =
    (await getLastImportedERC20TransferEvent(chain, contractAddress))
      ?.blockNumber || null;
  if (startBlock === null) {
    logger.debug(
      `[ERC20.T] No local data for ${chain}:${vault.id}, fetching contract creation info`
    );

    const { blockNumber } = await fetchContractCreationInfos(
      chain,
      contractAddress
    );
    startBlock = blockNumber;
  } else {
    logger.debug(
      `[ERC20.T] Found local data for ${chain}:${
        vault.id
      }, fetching data starting from block ${startBlock + 1}`
    );
    startBlock = startBlock + 1;
  }

  const endBlock = (
    await fetchCachedContractLastTransaction(chain, contractAddress)
  ).blockNumber;

  if (startBlock >= endBlock) {
    logger.info(`[ERC20.T] All data imported for ${contractAddress}`);
    return;
  }

  logger.info(
    `[ERC20.T] Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`
  );
  const { writeBatch, close } = await getERC20TransferStorageWriteStream(
    chain,
    contractAddress
  );

  const useExplorer = shouldUseExplorer(chain);

  try {
    if (useExplorer) {
      // Fuse and metis explorer requires an end block to be set
      // Error calling explorer https://explorer.fuse.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      // Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      const stopBlock = ["fuse", "metis"].includes(chain) ? endBlock : null;
      const stream = streamERC20TransferEventsFromExplorer(
        chain,
        contractAddress,
        startBlock,
        stopBlock
      );
      for await (const eventBatch of batchAsyncStream(stream, 1000)) {
        logger.debug("[ERC20.T] Writing batch");
        await writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            from: event.from,
            to: event.to,
            value: event.value,
          }))
        );
      }
    } else {
      const stream = streamERC20TransferEventsFromRpc(chain, contractAddress, {
        startBlock,
        endBlock,
        timeOrder: "timeline",
      });
      // write events as often as possible to avoid redoing too much work when pulling from RPC
      // we could have large period of data without event and that takes lots of time to pull
      // if we have an error in the meantime we have to redo all this work because we don't save
      // that there is no transactions in the block range. We could also do some checkpoints but
      // it's an exercise for the reader.
      for await (const event of stream) {
        logger.debug("[ERC20.T] Writing batch");
        await writeBatch([
          {
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            from: event.data.from,
            to: event.data.to,
            value: event.data.value,
          },
        ]);
      }
    }
  } finally {
    await close();
  }
}

runMain(main);
