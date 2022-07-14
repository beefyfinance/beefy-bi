import { erc20TransferFromStore } from "../lib/csv-store/csv-transfer-from-events";
import { contractCreationStore, contractLastTrxStore } from "../lib/json-store/contract-first-last-blocks";
import { streamERC20TransferEventsFromRpc } from "../lib/streamContractEventsFromRpc";
import { allChainIds, Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { sleep } from "../utils/async";
import { streamERC20TransferEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import { LOG_LEVEL, shouldUseExplorer } from "../utils/config";
import { runMain } from "../utils/process";
import { shuffle } from "lodash";
import { ArchiveNodeNeededError } from "../lib/shared-resources/shared-rpc";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: ["all"].concat(allChainIds), alias: "c", demand: true },
      strategyAddress: { type: "string", alias: "s", demand: false },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const strategyAddress = argv.strategyAddress ? normalizeAddress(argv.strategyAddress) : null;

  logger.info(`[ERC20.N.ST] Importing ${chain} ERC20 transfer events...`);
  const chains = chain === "all" ? shuffle(allChainIds) : [chain];

  // fetch all chains in parallel
  const chainPromises = chains.map(async (chain) => {
    try {
      await importChain(chain, strategyAddress);
    } catch (error) {
      logger.error(`[STRATS] Error importing ${chain} strategies: ${error}`);
      if (LOG_LEVEL === "trace") {
        console.log(error);
      }
    }
  });
  await Promise.allSettled(chainPromises);

  logger.info(`[ERC20.N.ST] Done importing ${chain} ERC20 transfer events, sleeping 4h`);
  await sleep(1000 * 60 * 60 * 4);
}

async function importChain(chain: Chain, strategyAddress: string | null) {
  try {
    const strategies = vaultStrategyStore.getAllStrategyAddresses(chain);
    for await (const strategy of strategies) {
      if (strategyAddress && strategy.implementation !== strategyAddress) {
        logger.debug(`[STRAT] Skipping strategy ${strategy.implementation}`);
        continue;
      }

      try {
        await importStrategyWNativeFrom(chain, strategy);
      } catch (e) {
        if (e instanceof ArchiveNodeNeededError) {
          logger.error(`[STRATS] Archive node needed, skipping vault ${chain}:${strategy.implementation}`);
        } else {
          logger.error(
            `[ERC20.N.ST] Error importing native transfers from. Skipping ${chain}:${
              strategy.implementation
            }. ${JSON.stringify(e)}`
          );
        }
      }
    }
  } catch (e) {
    logger.error(`[ERC20.N.ST] Error importing chain. Skipping ${chain}. ${JSON.stringify(e)}`);
  }
  logger.info(`[ERC20.N.ST] Done importing native TransferFrom for ${chain}`);
}

async function importStrategyWNativeFrom(chain: Chain, strategy: { implementation: string }) {
  const nativeAddress = getChainWNativeTokenAddress(chain);
  const contractAddress = normalizeAddress(strategy.implementation);

  logger.info(`[ERC20.N.ST] Processing ${chain}:${strategy.implementation}`);

  let startBlock =
    (await erc20TransferFromStore.getLastRow(chain, contractAddress, nativeAddress))?.blockNumber || null;
  if (startBlock === null) {
    logger.debug(`[ERC20.N.ST] No local data for ${chain}:${strategy.implementation}, fetching contract creation info`);

    const { blockNumber } = await contractCreationStore.fetchData(chain, contractAddress);
    startBlock = blockNumber;
  } else {
    logger.debug(
      `[ERC20.N.ST] Found local data for ${chain}:${strategy.implementation}, fetching data starting from block ${
        startBlock + 1
      }`
    );
    startBlock = startBlock + 1;
  }

  const endBlock = (await contractLastTrxStore.fetchData(chain, contractAddress)).blockNumber;

  if (startBlock >= endBlock) {
    logger.info(`[ERC20.N.ST] All data imported for ${contractAddress}`);
    return;
  }

  logger.info(`[ERC20.N.ST] Importing data for ${chain}:${strategy.implementation} (${startBlock} -> ${endBlock})`);
  const writer = await erc20TransferFromStore.getWriter(chain, contractAddress, nativeAddress);

  try {
    const useExplorer = shouldUseExplorer(chain);
    if (useExplorer) {
      // Fuse and metis explorer requires an end block to be set
      // Error calling explorer https://explorer.fuse.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      // Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      const stopBlock = ["fuse", "metis"].includes(chain) ? endBlock : null;
      const stream = streamERC20TransferEventsFromExplorer(
        chain,
        nativeAddress,
        startBlock,
        stopBlock,
        contractAddress
      );
      for await (const eventBatch of batchAsyncStream(stream, 1000)) {
        logger.debug("[ERC20.N.ST] Writing batch");
        await writer.writeBatch(
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
        logger.debug("[ERC20.N.ST] Writing batch");
        await writer.writeBatch(
          eventBatch.map((event) => ({
            blockNumber: event.blockNumber,
            datetime: event.datetime,
            to: event.data.to,
            value: event.data.value,
          }))
        );
      }
    }
  } finally {
    await writer.close();
  }
}

runMain(main);
