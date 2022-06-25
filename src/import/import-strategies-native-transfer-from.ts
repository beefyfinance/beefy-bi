import {
  getERC20TransferFromStorageWriteStream,
  getLastImportedERC20TransferFromBlockNumber,
} from "../lib/csv-transfer-from-events";
import {
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
import { getAllStrategyAddresses } from "../lib/csv-vault-strategy";
import { WNATIVE_ADDRESS } from "../utils/config";
import { runMain } from "../utils/process";
import { shuffle } from "lodash";

async function main() {
  const useExplorerFor: Chain[] = [
    "fantom",
    "cronos",
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
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: ["all"].concat(allChainIds), alias: "c", demand: true },
    }).argv;

  const chain = argv.chain as Chain | "all";

  logger.info(`[ERC20.N.ST] Importing ${chain} ERC20 transfer events...`);
  const chains = chain === "all" ? shuffle(allChainIds) : [chain];

  // find out which strategy we need data from
  for (const chain of chains) {
    const source = useExplorerFor.includes(chain) ? "explorer" : "rpc";
    try {
      const strategies = getAllStrategyAddresses(chain);
      for await (const strategy of strategies) {
        try {
          await importStrategyWNativeFrom(chain, source, strategy);
        } catch (e) {
          logger.error(
            `[ERC20.N.ST] Error importing native transfers from, from ${source}. Skipping ${chain}:${strategy}`
          );
          logger.error(e);
        }
      }
    } catch (e) {
      logger.error(
        `[ERC20.N.ST] Error importing chain from ${source}. Skipping ${chain}`
      );
      logger.error(e);
    }
  }

  logger.info(
    `[ERC20.N.ST] Done importing ${chain} ERC20 transfer events, sleeping 4h`
  );
  await sleep(1000 * 60 * 60 * 4);
}

async function importStrategyWNativeFrom(
  chain: Chain,
  source: "rpc" | "explorer",
  strategy: { implementation: string }
) {
  const nativeAddress = WNATIVE_ADDRESS[chain];
  const contractAddress = normalizeAddress(strategy.implementation);

  logger.info(`[ERC20.N.ST] Processing ${chain}:${strategy.implementation}`);

  let startBlock = await getLastImportedERC20TransferFromBlockNumber(
    chain,
    contractAddress,
    nativeAddress
  );
  if (startBlock === null) {
    logger.debug(
      `[ERC20.N.ST] No local data for ${chain}:${strategy.implementation}, fetching contract creation info`
    );

    const { blockNumber } = await fetchContractCreationInfos(
      chain,
      contractAddress
    );
    startBlock = blockNumber;
  } else {
    logger.debug(
      `[ERC20.N.ST] Found local data for ${chain}:${
        strategy.implementation
      }, fetching data starting from block ${startBlock + 1}`
    );
    startBlock = startBlock + 1;
  }

  const endBlock = (
    await fetchCachedContractLastTransaction(chain, contractAddress)
  ).blockNumber;

  if (startBlock >= endBlock) {
    logger.info(`[ERC20.N.ST] All data imported for ${contractAddress}`);
    return;
  }

  logger.info(
    `[ERC20.N.ST] Importing data for ${chain}:${strategy.implementation} (${startBlock} -> ${endBlock})`
  );
  const { writeBatch } = await getERC20TransferFromStorageWriteStream(
    chain,
    contractAddress,
    nativeAddress
  );

  if (source === "explorer") {
    const stream = streamERC20TransferEventsFromExplorer(
      chain,
      nativeAddress,
      startBlock,
      contractAddress
    );
    for await (const eventBatch of batchAsyncStream(stream, 1000)) {
      logger.verbose("[ERC20.N.ST] Writing batch");
      await writeBatch(
        eventBatch.map((event) => ({
          blockNumber: event.blockNumber,
          datetime: event.datetime,
          to: event.to,
          value: event.value,
        }))
      );
    }
  } else {
    const stream = streamERC20TransferEventsFromRpc(chain, nativeAddress, {
      startBlock,
      endBlock,
      timeOrder: "timeline",
      from: contractAddress,
    });
    for await (const eventBatch of batchAsyncStream(stream, 100)) {
      logger.verbose("[ERC20.N.ST] Writing batch");
      await writeBatch(
        eventBatch.map((event) => ({
          blockNumber: event.blockNumber,
          datetime: event.datetime,
          to: event.data.to,
          value: event.data.value,
        }))
      );
    }
  }
}

runMain(main);