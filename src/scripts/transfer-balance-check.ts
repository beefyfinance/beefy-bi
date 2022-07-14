import { shuffle, sortBy } from "lodash";
import { BeefyVault, vaultListStore.fetchData } from "../lib/fetch-if-not-found-locally";
import { allChainIds, Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { streamERC20TransferEvents } from "../lib/csv-store/csv-transfer-events";
import { logger } from "../utils/logger";
import yargs from "yargs";
import { ethers } from "ethers";
import { normalizeAddress } from "../utils/ethers";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: false, alias: "v" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId || null;

  for (const chain of chains) {
    await checkChain(chain, vaultId);
  }
}

async function checkChain(chain: Chain, vaultId: string | null) {
  const vaults = sortBy(await vaultListStore.fetchData(chain), (v) => v.token_name);
  for (const vault of vaults) {
    if (vaultId && vaultId !== vault.id) {
      logger.debug(`[TC] Skipping ${chain}:${vault.id}`);
      continue;
    } else {
      logger.info(`[TC] Checking ${chain}:${vault.id}`);
    }
    await checkVault(chain, vault);
  }
}

async function checkVault(chain: Chain, vault: BeefyVault) {
  const mintBurnAddr = normalizeAddress("0x0000000000000000000000000000000000000000");
  const contractAddress = vault.token_address;
  const eventStream = streamERC20TransferEvents(chain, contractAddress);

  const investorBalances: Record<string, ethers.BigNumber> = {};

  for await (const event of eventStream) {
    // check both address are normalized
    if (event.from !== normalizeAddress(event.from)) {
      logger.error(`[TC] Invalid event.from ${event.from}, proper value should be ${normalizeAddress(event.from)}`);
    }
    if (event.to !== normalizeAddress(event.to)) {
      logger.error(`[TC] Invalid event.to ${event.to}, proper value should be ${normalizeAddress(event.to)}`);
    }

    // now compute balances
    const amount = ethers.BigNumber.from(event.value);
    const diffs: { investorAddress: string; amount: ethers.BigNumber }[] = [
      {
        investorAddress: event.to,
        amount: amount,
      },
      {
        investorAddress: event.from,
        amount: amount.mul(-1),
      },
    ];

    // apply diffs
    for (const diff of diffs) {
      const investorAddress = diff.investorAddress;
      if (!investorBalances[investorAddress]) {
        investorBalances[investorAddress] = ethers.BigNumber.from(0);
      }
      investorBalances[investorAddress] = investorBalances[investorAddress].add(diff.amount);

      // balance should only be negative for the mint burn address
      if (investorBalances[investorAddress].lt(0) && investorAddress !== mintBurnAddr) {
        logger.error(`[TC] ${chain}:${vault.id} ${investorAddress} has negative balance`);
      }
    }

    // check total balance, should be zero
    const total = Object.values(investorBalances).reduce((acc, cur) => acc.add(cur), ethers.BigNumber.from(0));
    if (!total.eq(0)) {
      logger.error(`[TC] ${chain}:${vault.id} total balance is not zero`);
    }
  }
}

runMain(main);
