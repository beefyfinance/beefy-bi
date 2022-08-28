import { sortBy } from "lodash";
import { allChainIds, Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { erc20TransferStore } from "../lib/csv-store/csv-transfer-events";
import { ppfsStore } from "../lib/csv-store/csv-vault-ppfs";
import { erc20TransferFromStore } from "../lib/csv-store/csv-transfer-from-events";
import { vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import { LOG_LEVEL } from "../utils/config";
import { blockSamplesStore } from "../lib/csv-store/csv-block-samples";
import { vaultListStore } from "../beefy/connector/vault-list";
import { feeRecipientsStore } from "../lib/beefy/fee-recipients";
import { contractLastTrxStore } from "../lib/json-store/contract-first-last-blocks";
import { BeefyVault } from "../types/beefy";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
    }).argv;

  const chain = argv.chain as Chain | "all";

  const chainsToImport = chain === "all" ? allChainIds : [chain];
  const allPromises = sortBy(chainsToImport, (chain) => chain).map(async (chain) => {
    try {
      await checkChain(chain);
    } catch (e) {
      logger.error(`[CC] Error checking ${chain}. Skipping. ${e}`);
      if (LOG_LEVEL === "trace") {
        console.log(e);
      }
    }
  });
  await Promise.allSettled(allPromises);
}

async function checkChain(chain: Chain) {
  await checkBlockSamples(chain);

  const vaults = sortBy(await vaultListStore.getLocalData(chain), (v) => v.token_name);
  for (const vault of vaults) {
    await checkVault(chain, vault);
  }
}

async function checkBlockSamples(chain: Chain) {
  const now = new Date();
  const oneDay = 1000 * 60 * 60 * 24;
  const samplingPeriod = "4hour";
  const latestBlock = await blockSamplesStore.getLastRow(chain, samplingPeriod);

  if (latestBlock === null) {
    logger.error(`[CC] No block samples for ${chain}:${samplingPeriod}`);
  } else if (now.getTime() - latestBlock.datetime.getTime() > oneDay * 2) {
    logger.warn(`[CC] Last block sample is too old ${chain}:${samplingPeriod}: ${JSON.stringify(latestBlock)}`);
  } else {
    logger.verbose(`[CC] Block samples are ok for ${chain}:${samplingPeriod}`);
  }
}

async function checkVault(chain: Chain, vault: BeefyVault) {
  const now = new Date();
  const oneDay = 1000 * 60 * 60 * 24;
  const contractAddress = vault.token_address;

  // check we got a not-too-old transaction history
  const latestTransfer = await erc20TransferStore.getLastRow(chain, contractAddress);
  if (latestTransfer === null) {
    logger.error(`[CC] No ERC20 moo token transfer events found for vault ${chain}:${contractAddress}`);
  } else if (now.getTime() - latestTransfer.datetime.getTime() > oneDay) {
    // check against last transaction
    const lastTrx = await contractLastTrxStore.fetchData(chain, contractAddress);
    if (lastTrx.datetime.getTime() - latestTransfer.datetime.getTime() > oneDay) {
      logger.warn(
        `[CC] ERC20 last transfer is too old for vault ${chain}:${contractAddress}: ${JSON.stringify(latestTransfer)}`
      );
    } else {
      logger.verbose(`[CC] Vault transfers are OK for ${chain}:${contractAddress}`);
    }
  } else {
    logger.verbose(`[CC] Vault transfers are OK for ${chain}:${contractAddress}`);
  }

  // check we get a recent ppfs
  const sampling = "4hour";
  const latestPPFS = await ppfsStore.getLastRow(chain, contractAddress, sampling);
  if (latestPPFS === null) {
    logger.error(`[CC] No PPFS found for vault ${chain}:${contractAddress} and sampling ${sampling}`);
  } else if (now.getTime() - latestPPFS.datetime.getTime() > oneDay * 2) {
    logger.warn(`[CC] PPFS is too old for vault ${chain}:${contractAddress}: ${JSON.stringify(latestPPFS)}`);
  } else {
    logger.verbose(`[CC] Vault PPFS are OK for ${chain}:${contractAddress}`);
  }

  // check we got the vault strategies
  const latestStrat = await vaultStrategyStore.getLastRow(chain, contractAddress);
  if (latestStrat === null) {
    logger.error(`[CC] No Strategies found for vault ${chain}:${contractAddress}`);
  } else {
    logger.verbose(`[CC] Strategies list OK for vault ${chain}:${contractAddress}`);
  }

  // for each strategy
  const rows = vaultStrategyStore.getReadIterator(chain, contractAddress);
  for await (const strat of rows) {
    const strategyAddress = strat.implementation;

    // check we got transfer froms
    const wnative = getChainWNativeTokenAddress(chain);
    const lastTransferFrom = await erc20TransferFromStore.getLastRow(chain, strategyAddress, wnative);
    if (lastTransferFrom === null) {
      logger.error(`[CC] No TransferFrom events found for vault ${chain}:${strategyAddress}}`);
    } else if (now.getTime() - lastTransferFrom.datetime.getTime() > oneDay) {
      // check against last transaction
      const lastTrx = await contractLastTrxStore.fetchData(chain, contractAddress);
      if (lastTrx.datetime.getTime() - lastTransferFrom.datetime.getTime() > oneDay) {
        logger.warn(
          `[CC] TransferFrom events too old for vault ${chain}:${strategyAddress}: ${JSON.stringify(lastTransferFrom)}`
        );
      } else {
        logger.verbose(`[CC] TransferFrom events ok for ${chain}:${strategyAddress}`);
      }
    } else {
      logger.verbose(`[CC] TransferFrom events ok for ${chain}:${strategyAddress}`);
    }

    // check we got fee recipients
    const feeRecipients = feeRecipientsStore.getLocalData(chain, strategyAddress);
    if (feeRecipients === null) {
      logger.error(`[CC] No fee recipients found for vault ${chain}:${strategyAddress}`);
    } else {
      logger.verbose(`[CC] fee recipients ok for ${chain}:${strategyAddress}`);
    }
  }
}

runMain(main);
