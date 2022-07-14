import { Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { logger } from "../utils/logger";
import { ethers } from "ethers";
import { normalizeAddress } from "../utils/ethers";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";
import { erc20TransferStore } from "../lib/csv-store/csv-transfer-events";
import { BeefyVault } from "../types/beefy";

const main = foreachVaultCmd({
  loggerScope: "TC",
  additionalOptions: {},
  work: (_, chain, vault) => checkVault(chain, vault),
  shuffle: false,
  parallelize: false,
});

async function checkVault(chain: Chain, vault: BeefyVault) {
  const mintBurnAddr = normalizeAddress("0x0000000000000000000000000000000000000000");
  const contractAddress = vault.token_address;
  const events = erc20TransferStore.getReadIterator(chain, contractAddress);

  const investorBalances: Record<string, ethers.BigNumber> = {};

  for await (const event of events) {
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
