import { flatten, sortBy } from "lodash";
import { allChainIds, Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { erc20TransferStore } from "../lib/csv-store/csv-transfer-events";
import { ppfsStore } from "../lib/csv-store/csv-vault-ppfs";
import { BeefyVaultV6StrategiesData, vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { logger } from "../utils/logger";
import yargs from "yargs";
import * as path from "path";
import { DATA_DIRECTORY } from "../utils/config";
import { makeDataDirRecursive } from "../utils/make-data-dir-recursive";
import * as fs from "fs";
import { SamplingPeriod } from "../types/sampling";
import { getChainWNativeTokenOracleId } from "../utils/addressbook";
import { oraclePriceStore } from "../lib/csv-store/csv-oracle-price";
import { vaultListStore } from "../lib/beefy/vault-list";
import { BeefyVault } from "../types/beefy";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";

interface DataCoverageReportRow {
  chain: Chain;
  vault_id: string;
  want_oracle_id: string;
  moo_token_name: string;
  moo_token_address: string;
  moo_token_decimals: number;
  want_address: string;
  want_decimals: number;
  vault_creation_datetime: Date | null;
  vault_creation_block_number: number | null;
  vault_creation_transaction: string | null;
  first_moo_erc_20_transfer_datetime: Date | null;
  first_moo_erc_20_transfer_block_number: number | null;
  last_moo_erc_20_transfer_datetime: Date | null;
  last_moo_erc_20_transfer_block_number: number | null;
  want_oracle_first_price_datetime: Date | null;
  wnative_oracle_first_price_datetime: Date | null;
  want_oracle_last_price_datetime: Date | null;
  wnative_oracle_last_price_datetime: Date | null;
  first_ppfs_datetime: Date | null;
  first_ppfs_block_number: number | null;
  last_ppfs_datetime: Date | null;
  last_ppfs_block_number: number | null;
  strategies: BeefyVaultV6StrategiesData[] | null;
}

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

  const reportPromises = chains.map((chain) => getChainReport(chain, vaultId));
  const result = await Promise.allSettled(reportPromises);
  const reportRows = flatten(
    result.map((r) => {
      if (r.status === "fulfilled") {
        return r.value;
      } else {
        logger.error(r.reason);
        return [];
      }
    })
  );

  const filePath = path.join(DATA_DIRECTORY, "report", "data-coverage.jsonl");
  await makeDataDirRecursive(filePath);
  await fs.promises.writeFile(filePath, reportRows.map((row) => JSON.stringify(row)).join("\n"));
}

async function getChainReport(chain: Chain, vaultId: string | null): Promise<DataCoverageReportRow[]> {
  const vaults = sortBy(await vaultListStore.fetchData(chain), (v) => v.token_name);

  const reportRows: DataCoverageReportRow[] = [];
  for (const vault of vaults) {
    if (vaultId && vaultId !== vault.id) {
      logger.debug(`[DCR] Skipping ${chain}:${vault.id}`);
      continue;
    } else {
      logger.info(`[DCR] Checking ${chain}:${vault.id}`);
    }
    const report = await getVaultCoverageReport(chain, vault);
    reportRows.push(report);
  }
  return reportRows;
}

async function getVaultCoverageReport(chain: Chain, vault: BeefyVault): Promise<DataCoverageReportRow> {
  const reportRow: DataCoverageReportRow = {
    chain,
    vault_id: vault.id,
    moo_token_name: vault.token_name,
    moo_token_address: vault.token_address,
    moo_token_decimals: vault.token_decimals,
    want_address: vault.want_address,
    want_decimals: vault.want_decimals,
    want_oracle_id: vault.price_oracle.want_oracleId,
    vault_creation_datetime: null,
    vault_creation_block_number: null,
    vault_creation_transaction: null,
    first_moo_erc_20_transfer_datetime: null,
    first_moo_erc_20_transfer_block_number: null,
    last_moo_erc_20_transfer_datetime: null,
    last_moo_erc_20_transfer_block_number: null,
    want_oracle_first_price_datetime: null,
    want_oracle_last_price_datetime: null,
    wnative_oracle_first_price_datetime: null,
    wnative_oracle_last_price_datetime: null,
    first_ppfs_datetime: null,
    first_ppfs_block_number: null,
    last_ppfs_datetime: null,
    last_ppfs_block_number: null,
    strategies: null,
  };
  const contractAddress = vault.token_address;
  const ppfsSamplingPeriod: SamplingPeriod = "4hour";
  const priceSamplingPeriod: SamplingPeriod = "15min";
  const wantOracleId = vault.price_oracle.want_oracleId;
  const wnativeOracleId = getChainWNativeTokenOracleId(chain);

  try {
    /**
     * VAULT CREATION
     */
    // vault creation
    const creationInfos = await contractCreationStore.fetchData(chain, contractAddress);
    reportRow.vault_creation_datetime = creationInfos?.datetime || null;
    reportRow.vault_creation_block_number = creationInfos?.blockNumber || null;
    reportRow.vault_creation_transaction = creationInfos?.transactionHash || null;

    /**
     * ERC20 Transfer events
     */
    // first transfer
    const firstTransferEvent = await erc20TransferStore.getFirstRow(chain, contractAddress);
    reportRow.first_moo_erc_20_transfer_datetime = firstTransferEvent?.datetime || null;
    reportRow.first_moo_erc_20_transfer_block_number = firstTransferEvent?.blockNumber || null;

    // last transfer
    const lastTransferEvent = await erc20TransferStore.getLastRow(chain, contractAddress);
    reportRow.last_moo_erc_20_transfer_datetime = lastTransferEvent?.datetime || null;
    reportRow.last_moo_erc_20_transfer_block_number = lastTransferEvent?.blockNumber || null;

    /**
     * WANT oracle price
     */

    // first want oracle price
    const firstWantPrice = await oraclePriceStore.getFirstRow(wantOracleId, priceSamplingPeriod);
    reportRow.want_oracle_first_price_datetime = firstWantPrice?.datetime || null;

    // last want oracle price
    const lastWantPrice = await oraclePriceStore.getLastRow(wantOracleId, priceSamplingPeriod);
    reportRow.want_oracle_last_price_datetime = lastWantPrice?.datetime || null;

    /**
     * WNATIVE oracle price
     */

    // first wnative oracle price
    const firstWNativePrice = await oraclePriceStore.getFirstRow(wnativeOracleId, priceSamplingPeriod);
    reportRow.wnative_oracle_first_price_datetime = firstWNativePrice?.datetime || null;

    // last wnative oracle price
    const lastWNativePrice = await oraclePriceStore.getLastRow(wnativeOracleId, priceSamplingPeriod);
    reportRow.wnative_oracle_last_price_datetime = lastWNativePrice?.datetime || null;

    /**
     * PPFS
     */

    // first
    const firstPPFS = await ppfsStore.getFirstRow(chain, contractAddress, ppfsSamplingPeriod);
    reportRow.first_ppfs_datetime = firstPPFS?.datetime || null;
    reportRow.first_ppfs_block_number = firstPPFS?.blockNumber || null;

    // last
    const lastPPFS = await ppfsStore.getLastRow(chain, contractAddress, ppfsSamplingPeriod);
    reportRow.last_ppfs_datetime = lastPPFS?.datetime || null;
    reportRow.last_ppfs_block_number = lastPPFS?.blockNumber || null;

    /**
     * STRATEGIES
     */
    const strategies: BeefyVaultV6StrategiesData[] = [];
    const rows = vaultStrategyStore.getReadIterator(chain, contractAddress);
    for await (const strategy of rows) {
      strategies.push(strategy);
    }
    reportRow.strategies = strategies.length > 0 ? strategies : null;
  } catch (e) {
    logger.error(`[DCR] Error generating coverage report for ${chain}:${vault.id} : ${JSON.stringify(e)}`);
  }
  return reportRow;
}

runMain(main);
