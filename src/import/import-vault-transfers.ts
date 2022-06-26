import {
  getERC20TransferStorageWriteStream,
  getLastImportedERC20TransferEvent,
} from "../lib/csv-transfer-events";
import {
  fetchBeefyVaultAddresses,
  fetchCachedContractLastTransaction,
  fetchContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import { streamERC20TransferEventsFromRpc } from "../lib/streamContractEventsFromRpc";
import { allChainIds } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { streamERC20TransferEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { shuffle } from "lodash";
import { runMain } from "../utils/process";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: allChainIds, alias: "c", demand: true },
      source: {
        choices: ["rpc", "explorer"],
        alias: "s",
        demand: true,
      },
    }).argv;

  const chain = argv.chain;

  logger.info(`[ERC20.T] Importing ${chain} ERC20 transfer events...`);
  // find out which vaults we need to parse
  const vaults = shuffle(await fetchBeefyVaultAddresses(chain));

  // for each vault, find out the creation date or last imported transfer
  for (const vault of vaults) {
    const contractAddress = normalizeAddress(vault.token_address);
    logger.info(
      `[ERC20.T] Processing ${chain}:${vault.id} (${contractAddress})`
    );

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
      continue;
    }

    logger.info(
      `[ERC20.T] Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`
    );
    const { writeBatch } = await getERC20TransferStorageWriteStream(
      chain,
      contractAddress
    );

    if (argv.source === "explorer") {
      const stream = streamERC20TransferEventsFromExplorer(
        chain,
        contractAddress,
        startBlock
      );
      for await (const eventBatch of batchAsyncStream(stream, 1000)) {
        logger.verbose("[ERC20.T] Writing batch");
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
      for await (const eventBatch of batchAsyncStream(stream, 10)) {
        logger.verbose("[ERC20.T] Writing batch");
        await writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            from: event.data.from,
            to: event.data.to,
            value: event.data.value,
          }))
        );
      }
    }
  }

  logger.info(
    `[ERC20.T] Done importing ${chain} ERC20 transfer events, sleeping 4h`
  );
  await sleep(1000 * 60 * 60 * 4);
}

runMain(main);
