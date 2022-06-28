import { shuffle, sortBy } from "lodash";
import {
  BeefyVault,
  fetchBeefyVaultList,
  fetchCachedContractLastTransaction,
} from "../lib/fetch-if-not-found-locally";
import { allChainIds, Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { getLastImportedERC20TransferEvent } from "../lib/csv-transfer-events";
import { getLastImportedBeefyVaultV6PPFSData } from "../lib/csv-vault-ppfs";
import { getLastImportedERC20TransferFromEvent } from "../lib/csv-transfer-from-events";
import { getLastImportedSampleBlockData } from "../lib/csv-block-samples";
import {
  getLastImportedBeefyVaultV6Strategy,
  streamVaultStrategies,
} from "../lib/csv-vault-strategy";

import { logger } from "../utils/logger";
import yargs from "yargs";
import { getChainWNativeTokenAddress } from "../utils/addressbook";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  for (const chain of chains) {
    await checkChain(chain);
  }
}

async function checkChain(chain: Chain) {
  await checkBlockSamples(chain);

  const vaults = sortBy(await fetchBeefyVaultList(chain), (v) => v.token_name);
  for (const vault of vaults) {
    await checkVault(chain, vault);
  }
}

async function checkBlockSamples(chain: Chain) {
  const now = new Date();
  const oneDay = 1000 * 60 * 60 * 24;
  const samplingPeriod = "4hour";
  const latestBlock = await getLastImportedSampleBlockData(
    chain,
    samplingPeriod
  );

  if (latestBlock === null) {
    logger.error(`[CC] No block samples for ${chain}:${samplingPeriod}`);
  } else if (now.getTime() - latestBlock.datetime.getTime() > oneDay * 2) {
    logger.warn(
      `[CC] Last block sample is too old ${chain}:${samplingPeriod}: ${JSON.stringify(
        latestBlock
      )}`
    );
  } else {
    logger.verbose(`[CC] Block samples are ok for ${chain}:${samplingPeriod}`);
  }
}

async function checkVault(chain: Chain, vault: BeefyVault) {
  const now = new Date();
  const oneDay = 1000 * 60 * 60 * 24;
  const contractAddress = vault.token_address;

  // check we got a not-too-old transaction history
  const latestTransfer = await getLastImportedERC20TransferEvent(
    chain,
    contractAddress
  );
  if (latestTransfer === null) {
    logger.error(
      `[CC] No ERC20 moo token transfer events found for vault ${chain}:${contractAddress}`
    );
  } else if (now.getTime() - latestTransfer.datetime.getTime() > oneDay) {
    // check against last transaction
    const lastTrx = await fetchCachedContractLastTransaction(
      chain,
      contractAddress
    );
    if (
      lastTrx.datetime.getTime() - latestTransfer.datetime.getTime() >
      oneDay
    ) {
      logger.warn(
        `[CC] ERC20 last transfer is too old for vault ${chain}:${contractAddress}: ${JSON.stringify(
          latestTransfer
        )}`
      );
    } else {
      logger.verbose(
        `[CC] Vault transfers are OK for ${chain}:${contractAddress}`
      );
    }
  } else {
    logger.verbose(
      `[CC] Vault transfers are OK for ${chain}:${contractAddress}`
    );
  }

  // check we get a recent ppfs
  const sampling = "4hour";
  const latestPPFS = await getLastImportedBeefyVaultV6PPFSData(
    chain,
    contractAddress,
    sampling
  );
  if (latestPPFS === null) {
    logger.error(
      `[CC] No PPFS found for vault ${chain}:${contractAddress} and sampling ${sampling}`
    );
  } else if (now.getTime() - latestPPFS.datetime.getTime() > oneDay * 2) {
    logger.warn(
      `[CC] PPFS is too old for vault ${chain}:${contractAddress}: ${JSON.stringify(
        latestPPFS
      )}`
    );
  } else {
    logger.verbose(`[CC] Vault PPFS are OK for ${chain}:${contractAddress}`);
  }

  // check we got the vault strategies
  const latestStrat = await getLastImportedBeefyVaultV6Strategy(
    chain,
    contractAddress
  );
  if (latestStrat === null) {
    logger.error(
      `[CC] No Strategies found for vault ${chain}:${contractAddress}`
    );
  } else {
    logger.verbose(
      `[CC] Strategies list OK for vault ${chain}:${contractAddress}`
    );
  }

  // for each strategy, check we got transfer froms
  const stratStream = streamVaultStrategies(chain, contractAddress);
  for await (const strat of stratStream) {
    const fromAddress = strat.implementation;
    const wnative = getChainWNativeTokenAddress(chain);
    const lastTransferFrom = await getLastImportedERC20TransferFromEvent(
      chain,
      fromAddress,
      wnative
    );
    if (lastTransferFrom === null) {
      logger.error(
        `[CC] No TransferFrom events found for vault ${chain}:${fromAddress}}`
      );
    } else if (now.getTime() - lastTransferFrom.datetime.getTime() > oneDay) {
      // check against last transaction
      const lastTrx = await fetchCachedContractLastTransaction(
        chain,
        contractAddress
      );
      if (
        lastTrx.datetime.getTime() - lastTransferFrom.datetime.getTime() >
        oneDay
      ) {
        logger.warn(
          `[CC] TransferFrom events too old for vault ${chain}:${fromAddress}: ${JSON.stringify(
            lastTransferFrom
          )}`
        );
      } else {
        logger.verbose(
          `[CC] TransferFrom events ok for ${chain}:${fromAddress}`
        );
      }
    } else {
      logger.verbose(`[CC] TransferFrom events ok for ${chain}:${fromAddress}`);
    }
  }
}

runMain(main);
