import { runMain } from "../utils/process";

import yargs from "yargs";
import { normalizeAddress } from "../utils/ethers";
import { fetchBeefyPPFS } from "../lib/csv-vault-ppfs";
import { allChainIds } from "../types/chain";
import { Chain } from "../types/chain";
import { fetchBeefyVaultList } from "../lib/fetch-if-not-found-locally";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: allChainIds, alias: "c", demand: true },
      vaultId: { type: "string", alias: "a", demand: true },
      blockNumber: { type: "number", alias: "b", demand: true },
    }).argv;

  const chain = argv.chain as Chain;

  const vaults = await fetchBeefyVaultList(chain);
  const vault = vaults.find((v) => v.id === argv.vaultId);
  if (!vault) {
    throw new Error(`[${chain}] Vault not found: ${argv.vaultId}`);
  }

  const ppfs = await fetchBeefyPPFS(
    chain,
    vault.token_address,
    argv.blockNumber
  );
  console.log(ppfs);
  console.log(ppfs.toString());
  console.log(ppfs.toHexString());
}

runMain(main);
