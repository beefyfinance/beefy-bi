import * as fs from "fs";
import * as path from "path";
import { Chain } from "../types/chain";
import { logger } from "../utils/logger";
import { runMain } from "../utils/process";
import { DATA_DIRECTORY, _forceUseSource } from "../utils/config";
import { normalizeAddress } from "../utils/ethers";
import { deleteAllVaultData } from "../lib/csv-store/csv-vault";
import { BeefyVault } from "../types/beefy";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";
import { streamERC20TransferEventsFromRpc } from "../lib/streamContractEventsFromRpc";
import { option } from "yargs";
import { ERC20EventData, erc20TransferStore } from "../lib/csv-store/csv-transfer-events";
import { streamERC20TransferEventsFromExplorer } from "../lib/streamContractEventsFromExplorer";
import { isEqual } from "lodash";

const main = foreachVaultCmd({
  loggerScope: "ERC20.T.FIX",
  additionalOptions: {
    source: { choices: ["explorer", "rpc"], demand: true, alias: "s" },
    dryrun: { type: "boolean", demand: true, default: true, alias: "d" },
  },
  work: (argv, chain, vault) => {
    const source = argv.source as "explorer" | "rpc";
    return fixVault(chain, vault, argv.dryrun, source);
  },
  shuffle: false,
  parallelize: false,
});

// for the actual target vault
// fetch the actual creation date from RPC using OwnershipTransfered logs
// if local creation date and RPC date are the same, do nothing
// otherwise
// write new creation date to local file
// find all strategies
// delete all files from all strategies
// delete all files for this vault

// manually purge db or full db reload

async function fixVault(chain: Chain, vault: BeefyVault, dryrun: boolean, source: "explorer" | "rpc") {
  const contractAddress = vault.token_address;

  const createInfos = await contractCreationStore.fetchData(chain, contractAddress);
  const lastLocalTransfer = await erc20TransferStore.getLastRow(chain, contractAddress);
  if (lastLocalTransfer === null) {
    logger.info(`[ERC20.T.FIX] No local transfer events for ${chain}:${vault.id}. Skipping.`);
    return;
  }
  const localTransfers = erc20TransferStore.getReadIterator(chain, contractAddress)[Symbol.asyncIterator]();
  const eventStream =
    source === "rpc"
      ? streamERC20TransferEventsFromRpc(chain, contractAddress, {
          startBlock: createInfos.blockNumber,
          timeOrder: "timeline",
        })
      : streamERC20TransferEventsFromExplorer(chain, contractAddress, createInfos.blockNumber, null);

  let transferListDiffers = false;

  const remoteTransfers: ERC20EventData[] = [];
  for await (const remoteTransfer of eventStream) {
    console.log({ remoteTransfer });
    const localNext = await localTransfers.next();
    if (localNext.done) {
      logger.debug(`[ERC20.T.FIX][${chain}:${vault.id}] Local transfer events exhausted.`);
      break;
    }
    remoteTransfers.push(remoteTransfer);
    const localTransfer = localNext.value;
    console.log({ remoteTransfer, localTransfer });

    if (!isEqual(localTransfer, remoteTransfer)) {
      logger.debug(`[ERC20.T.FIX][${chain}:${vault.id}] Local transfer event differs from remote.`);

      if (localTransfer.blockNumber > remoteTransfer.blockNumber) {
        logger.debug(`[ERC20.T.FIX][${chain}:${vault.id}] Found a remote event missing from the local file.`);
        continue;
      } else if (remoteTransfer.blockNumber > localTransfer.blockNumber) {
        logger.error(`[ERC20.T.FIX][${chain}:${vault.id}] Found a local event missing from the remote source.`);
        return;
      } else {
        logger.error(
          `[ERC20.T.FIX][${chain}:${vault.id}] Local and remote Event differs but we can't tell the difference.`
        );
        return;
      }
    }
  }
}

runMain(main);
