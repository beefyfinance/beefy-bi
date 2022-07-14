import { erc20TransferStore } from "../lib/csv-store/csv-transfer-events";
import { streamERC20TransferEventsFromRpc } from "../lib/streamContractEventsFromRpc";
import { Chain } from "../types/chain";
import { batchAsyncStream } from "../utils/batch";
import { normalizeAddress } from "../utils/ethers";
import { logger } from "../utils/logger";
import { sleep } from "../utils/async";
import { streamERC20TransferEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { runMain } from "../utils/process";
import { shouldUseExplorer } from "../utils/config";
import { contractCreationStore, contractLastTrxStore } from "../lib/json-store/contract-first-last-blocks";
import { BeefyVault } from "../types/beefy";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";

const main = foreachVaultCmd({
  loggerScope: "ERC20.T",
  additionalOptions: {},
  work: (_, chain, vault) => importVault(chain, vault),
  onFinish: async (argv) => {
    logger.info(`[ERC20.T] Done importing ERC20 transfer events, sleeping 4h`);
    await sleep(1000 * 60 * 60 * 4);
  },
  shuffle: true,
  parallelize: true,
});

async function importVault(chain: Chain, vault: BeefyVault) {
  const contractAddress = normalizeAddress(vault.token_address);
  logger.info(`[ERC20.T] Processing ${chain}:${vault.id} (${contractAddress})`);

  let startBlock = (await erc20TransferStore.getLastRow(chain, contractAddress))?.blockNumber || null;
  if (startBlock === null) {
    logger.debug(`[ERC20.T] No local data for ${chain}:${vault.id}, fetching contract creation info`);

    const { blockNumber } = await contractCreationStore.fetchData(chain, contractAddress);
    startBlock = blockNumber;
  } else {
    logger.debug(
      `[ERC20.T] Found local data for ${chain}:${vault.id}, fetching data starting from block ${startBlock + 1}`
    );
    startBlock = startBlock + 1;
  }

  const endBlock = (await contractLastTrxStore.fetchData(chain, contractAddress)).blockNumber;

  if (startBlock >= endBlock) {
    logger.info(`[ERC20.T] All data imported for ${contractAddress}`);
    return;
  }

  logger.info(`[ERC20.T] Importing data for ${chain}:${vault.id} (${startBlock} -> ${endBlock})`);
  const writer = await erc20TransferStore.getWriter(chain, contractAddress);

  const useExplorer = shouldUseExplorer(chain);

  try {
    if (useExplorer) {
      // Fuse and metis explorer requires an end block to be set
      // Error calling explorer https://explorer.fuse.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      // Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: toBlock","result":null,"status":"0"}
      const stopBlock = ["fuse", "metis"].includes(chain) ? endBlock : null;
      const stream = streamERC20TransferEventsFromExplorer(chain, contractAddress, startBlock, stopBlock);
      for await (const eventBatch of batchAsyncStream(stream, 1000)) {
        logger.debug("[ERC20.T] Writing batch");
        await writer.writeBatch(eventBatch);
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
        await writer.writeBatch([event]);
      }
    }
  } finally {
    await writer.close();
  }
}

runMain(main);
