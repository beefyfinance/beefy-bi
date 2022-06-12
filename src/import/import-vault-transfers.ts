import { _fetchContractFirstLastTrx } from "../lib/contract-transaction-infos";
import {
  getERC20TransferStorageWriteStream,
  getLastImportedERC20TransferBlockNumber,
} from "../lib/csv-transfer-events";
import {
  fetchBeefyVaultAddresses,
  fetchContractCreationInfos,
} from "../lib/fetch-if-not-found-locally";
import { streamERC20TransferEvents } from "../lib/streamContractEvents";
import { allChainIds, Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { backOff } from "exponential-backoff";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: allChainIds, alias: "c", demand: true },
    }).argv;

  const chain = argv.chain;

  logger.info(`Importing ${chain} ERC20 transfer events...`);
  // find out which vaults we need to parse
  const vaults = await fetchBeefyVaultAddresses(chain);

  // for each vault, find out the creation date or last imported transfer
  for (const vault of vaults) {
    const contractAddress = normalizeAddress(vault.token_address);
    logger.info(`Processing ${chain}:${vault.id} (${contractAddress})`);

    let startBlock = await getLastImportedERC20TransferBlockNumber(
      chain,
      contractAddress
    );
    if (startBlock === null) {
      logger.debug(
        `No local data for ${chain}:${vault.id}, fetching contract creation info`
      );

      const { blockNumber } = await fetchContractCreationInfos(
        chain,
        contractAddress
      );
      startBlock = blockNumber;
    } else {
      logger.debug(
        `Found local data for ${chain}:${
          vault.id
        }, fetching data starting from block ${startBlock + 1}`
      );
      startBlock = startBlock + 1;
    }

    const endBlock = (
      await backOff(
        () => _fetchContractFirstLastTrx(chain, contractAddress, "last"),
        {
          retry: async (error, attemptNumber) => {
            logger.info(
              `Error on attempt ${attemptNumber} fetching last trx of ${chain}:${contractAddress}: ${error}`
            );
            console.error(error);
            return true;
          },
          numOfAttempts: 10,
          startingDelay: 5000,
        }
      )
    ).blockNumber;

    if (startBlock >= endBlock) {
      logger.info(`All data imported for ${contractAddress}`);
      continue;
    }

    logger.info(
      `Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`
    );
    const stream = streamERC20TransferEvents(chain, contractAddress, {
      startBlock,
      endBlock,
      timeOrder: "timeline",
    });
    const { writeBatch } = await getERC20TransferStorageWriteStream(
      chain,
      contractAddress
    );

    for await (const eventBatch of batchAsyncStream(stream, 100)) {
      logger.verbose("Writing batch");
      await writeBatch(
        eventBatch.map((event) => ({
          blockNumber: event.blockNumber,
          from: event.data.from,
          to: event.data.to,
          value: event.data.value,
        }))
      );
    }
  }

  logger.info(`Done importing ${chain} ERC20 transfer events, sleeping 1h`);
  await sleep(1000 * 60 * 60);
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
