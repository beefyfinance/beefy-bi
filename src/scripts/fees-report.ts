import { flatten } from "lodash";
import { Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { erc20TransferFromStore } from "../lib/csv-store/csv-transfer-from-events";
import { feeRecipientsStore } from "../lib/beefy/fee-recipients";
import { BeefyVaultV6StrategiesData, vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { logger } from "../utils/logger";
import * as path from "path";
import { DATA_DIRECTORY, LOG_LEVEL } from "../utils/config";
import { makeDataDirRecursive } from "../utils/fs";
import * as fs from "fs";
import { SamplingPeriod } from "../types/sampling";
import { getChainWNativeTokenOracleId } from "../utils/addressbook";
import { oraclePriceStore } from "../lib/csv-store/csv-oracle-price";
import { BeefyVault } from "../types/beefy";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";
import { ethers } from "ethers";

interface FeeReportRow {
  chain: Chain;
  vault_id: string;
  native_oracle_id: string;
  strategies: BeefyVaultV6StrategiesData[] | null;
  start_date: Date;
  end_date: Date;
  total_strategist_fee_wnative: ethers.BigNumber;
  total_strategist_fee_usd: ethers.BigNumber;
  total_beefy_fee_wnative: ethers.BigNumber;
  total_beefy_fee_usd: ethers.BigNumber;
  total_caller_fee_wnative: ethers.BigNumber;
  total_caller_fee_usd: ethers.BigNumber;
  total_vault_compound_wnative: ethers.BigNumber;
  total_vault_compound_usd: ethers.BigNumber;
}

const main = foreachVaultCmd({
  loggerScope: "FR",
  additionalOptions: {},
  work: (_, chain, vault) => getVaultFeeReport(chain, vault),
  onFinish: async (_, results) => {
    const reportRows = flatten(Object.values(results));
    const filePath = path.join(DATA_DIRECTORY, "report", "fee-report.jsonl");
    await makeDataDirRecursive(filePath);
    await fs.promises.writeFile(filePath, reportRows.map((row) => JSON.stringify(row)).join("\n"));
  },
  shuffle: false,
  parallelize: false,
});

async function getVaultFeeReport(chain: Chain, vault: BeefyVault): Promise<FeeReportRow> {
  logger.debug(`[FR] Getting fee report for ${chain}:${vault.id}`);
  const contractAddress = vault.token_address;
  const priceSamplingPeriod: SamplingPeriod = "15min";
  const wnativeOracleId = getChainWNativeTokenOracleId(chain);

  const reportRow: FeeReportRow = {
    chain,
    vault_id: vault.id,
    native_oracle_id: wnativeOracleId,
    strategies: [],
    start_date: new Date(0),
    end_date: new Date(0),
    total_strategist_fee_wnative: ethers.BigNumber.from(0),
    total_strategist_fee_usd: ethers.BigNumber.from(0),
    total_beefy_fee_wnative: ethers.BigNumber.from(0),
    total_beefy_fee_usd: ethers.BigNumber.from(0),
    total_caller_fee_wnative: ethers.BigNumber.from(0),
    total_caller_fee_usd: ethers.BigNumber.from(0),
    total_vault_compound_wnative: ethers.BigNumber.from(0),
    total_vault_compound_usd: ethers.BigNumber.from(0),
  };
  try {
    const strategies: BeefyVaultV6StrategiesData[] = [];
    const rows = vaultStrategyStore.getReadIterator(chain, contractAddress);
    for await (const strategy of rows) {
      strategies.push(strategy);
    }
    reportRow.strategies = strategies;
    logger.debug(`[FR] Found ${strategies.length} strategies for ${chain}:${vault.id}`);

    // get strategy address map
    const addressRoleMap: Record<string, "beefy" | "strategist"> = {};
    for (const strategy of strategies) {
      const feeRecipients = await feeRecipientsStore.getLocalData(chain, strategy.implementation);
      if (!feeRecipients) {
        logger.debug(`[FR] No fee recipients found for ${strategy.implementation}`);
        continue;
      }
      if (feeRecipients.beefyFeeRecipient) {
        addressRoleMap[feeRecipients.beefyFeeRecipient] = "beefy";
      }
      addressRoleMap[feeRecipients.strategist] = "strategist";
    }
    logger.verbose(`[FR] addressRoleMap for ${chain}:${vault.id}: ${JSON.stringify(addressRoleMap)}`);

    for (const strategy of strategies) {
      const nativeTransferFromStrategyRows = erc20TransferFromStore.getReadIterator(
        chain,
        contractAddress,
        strategy.implementation
      );

      for await (const transfer of nativeTransferFromStrategyRows) {
        const amount = ethers.BigNumber.from(transfer.value);
      }
    }
  } catch (e) {
    logger.error(`[DCR] Error generating coverage report for ${chain}:${vault.id} : ${e}`);
    if (LOG_LEVEL === "trace") {
      console.log(e);
    }
  }
  return reportRow;
}

runMain(main);
