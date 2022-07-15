import { flatten, groupBy, keyBy, maxBy } from "lodash";
import { Chain } from "../types/chain";
import { runMain } from "../utils/process";
import { ERC20TransferFromEventData, erc20TransferFromStore } from "../lib/csv-store/csv-transfer-from-events";
import { feeRecipientsStore } from "../lib/beefy/fee-recipients";
import { BeefyVaultV6StrategiesData, vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { logger } from "../utils/logger";
import * as path from "path";
import { DATA_DIRECTORY, LOG_LEVEL } from "../utils/config";
import { makeDataDirRecursive } from "../utils/fs";
import * as fs from "fs";
import { SamplingPeriod } from "../types/sampling";
import {
  getChainWNativeTokenAddress,
  getChainWNativeTokenDecimals,
  getChainWNativeTokenOracleId,
  getChainWNativeTokenSymbol,
} from "../utils/addressbook";
import { oraclePriceStore } from "../lib/csv-store/csv-oracle-price";
import { BeefyVault } from "../types/beefy";
import { contractCreationStore } from "../lib/json-store/contract-first-last-blocks";
import { foreachVaultCmd } from "../utils/foreach-vault-cmd";
import { ethers } from "ethers";
import { normalizeAddress } from "../utils/ethers";
import BigNumber from "bignumber.js";

interface FeeReportRow {
  chain: Chain;
  vault_id: string;
  wnative_oracle_id: string;
  wnative_symbol: string;
  wnative_decimals: number;
  strategies: string[] | null;
  transfer_from_count: Record<string, number>;
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
    logger.info(`[Fees Report] Writing report to file`);
    const reportRows = flatten(Object.values(results)).map((reportRow) => ({
      ...reportRow,
      total_strategist_fee_wnative: formatBigNumber(reportRow.total_strategist_fee_wnative, reportRow.wnative_decimals),
      total_strategist_fee_usd: reportRow.total_strategist_fee_usd.toString(),
      total_beefy_fee_wnative: formatBigNumber(reportRow.total_beefy_fee_wnative, reportRow.wnative_decimals),
      total_beefy_fee_usd: reportRow.total_beefy_fee_usd.toString(),
      total_caller_fee_wnative: formatBigNumber(reportRow.total_caller_fee_wnative, reportRow.wnative_decimals),
      total_caller_fee_usd: reportRow.total_caller_fee_usd.toString(),
      total_vault_compound_wnative: formatBigNumber(reportRow.total_vault_compound_wnative, reportRow.wnative_decimals),
      total_vault_compound_usd: reportRow.total_vault_compound_usd.toString(),
    }));
    let filePath = path.join(DATA_DIRECTORY, "report", "fee-report.jsonl");
    await makeDataDirRecursive(filePath);
    await fs.promises.writeFile(filePath, reportRows.map((row) => JSON.stringify(row)).join("\n"));

    filePath = path.join(DATA_DIRECTORY, "report", "fee-report.json");
    await makeDataDirRecursive(filePath);
    await fs.promises.writeFile(filePath, JSON.stringify(reportRows, null, 2));
  },
  shuffle: false,
  parallelize: false,
});

async function getVaultFeeReport(chain: Chain, vault: BeefyVault): Promise<FeeReportRow> {
  logger.debug(`[FR] Getting fee report for ${chain}:${vault.id}`);
  const contractAddress = vault.token_address;
  const priceSamplingPeriod: SamplingPeriod = "15min";
  const wnativeOracleId = getChainWNativeTokenOracleId(chain);
  const wnativeTokenAddress = getChainWNativeTokenAddress(chain);
  const wnativeTokenSymbol = getChainWNativeTokenSymbol(chain);
  const wnativeTokenDecimals = getChainWNativeTokenDecimals(chain);

  const reportRow: FeeReportRow = {
    chain,
    vault_id: vault.id,
    wnative_oracle_id: wnativeOracleId,
    wnative_symbol: wnativeTokenSymbol,
    wnative_decimals: wnativeTokenDecimals,
    strategies: [],
    transfer_from_count: {},
    start_date: new Date(),
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
    reportRow.strategies = strategies.map((s) => s.implementation);
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
        addressRoleMap[normalizeAddress(feeRecipients.beefyFeeRecipient)] = "beefy";
      }
      addressRoleMap[normalizeAddress(feeRecipients.strategist)] = "strategist";
    }
    logger.verbose(`[FR] addressRoleMap for ${chain}:${vault.id}: ${JSON.stringify(addressRoleMap)}`);
    const roleAddressMap = Object.entries(addressRoleMap).reduce(
      (acc, [address, role]) => Object.assign(acc, { [role]: address }),
      {} as Record<"beefy" | "strategist", string>
    );

    for (const strategy of strategies) {
      reportRow.transfer_from_count[strategy.implementation] = 0;

      const nativeTransferFromStrategyRows = erc20TransferFromStore.getReadIterator(
        chain,
        strategy.implementation,
        wnativeTokenAddress
      );

      let currentBlockNumber: number = 0;
      let blockTransfers: ERC20TransferFromEventData[] = [];
      for await (const transfer of nativeTransferFromStrategyRows) {
        if (reportRow.start_date.getTime() > transfer.datetime.getTime()) {
          reportRow.start_date = transfer.datetime;
        }
        if (reportRow.end_date.getTime() < transfer.datetime.getTime()) {
          reportRow.end_date = transfer.datetime;
        }
        reportRow.transfer_from_count[strategy.implementation] += 1;

        if (transfer.blockNumber === currentBlockNumber) {
          blockTransfers.push(transfer);
          continue;
        }

        // now we have a new block of transfers
        // first handle the current transfer
        const transferBatch = blockTransfers;
        blockTransfers = [transfer];
        currentBlockNumber = transfer.blockNumber;

        // then, process the batch

        // if we have 4 transfers exactly, it's easy
        // there should be 2 addresses in the address map (strategist and beefy)
        // there should be 1 transfer larger than the other ones, it's the compound
        // the last one is the caller
        // with 3 transfers, there is no wnative compound but the logic is the same
        if (transferBatch.length === 4 || transferBatch.length === 3) {
          const cleanTransferBatch = transferBatch.map((transfer) => ({
            to: normalizeAddress(transfer.to),
            value: ethers.BigNumber.from(transfer.value),
          }));
          // sum by address
          const transfersBatchByAddr = groupBy(cleanTransferBatch, (transfer) => transfer.to);
          const transferBatchByAddr = Object.entries(transfersBatchByAddr).reduce(
            (acc, [to, transfers]) =>
              Object.assign(acc, {
                [to]: transfers.reduce((acc, transfer) => acc.add(transfer.value), ethers.BigNumber.from(0)),
              }),
            {} as Record<string, ethers.BigNumber>
          );
          const beefyTransferAmount = transferBatchByAddr[roleAddressMap["beefy"]];
          const strategistTransferAmount = transferBatchByAddr[roleAddressMap["strategist"]];
          // find the biggest transfer if we have 4 transfers, otherwise there is no compound
          let compoundAddr: string | null = null;
          let compoundAmount = ethers.BigNumber.from(0);
          if (transferBatch.length === 4) {
            for (const [addr, amount] of Object.entries(transferBatchByAddr)) {
              if (addr !== roleAddressMap["beefy"] && addr !== roleAddressMap["strategist"]) {
                if (amount.gt(compoundAmount)) {
                  compoundAddr = addr;
                  compoundAmount = amount;
                }
              }
            }
            if (compoundAddr === null) {
              throw new Error(`[FR] No compound address found for ${strategy.implementation}`);
            }
          }
          // last one in the batch should be the caller
          const callerTransfer = cleanTransferBatch.find(
            (transfer) =>
              !(
                transfer.to === roleAddressMap["beefy"] ||
                // special case for when the strategist is the caller
                (transfer.to === roleAddressMap["strategist"] && transfer.value.eq(strategistTransferAmount)) ||
                transfer.to === compoundAddr
              )
          );
          if (!callerTransfer) {
            throw new Error(
              `[FR] No caller transfer found for ${strategy.implementation} and block ${transferBatch[0].blockNumber}`
            );
          }
          reportRow.total_beefy_fee_wnative = reportRow.total_beefy_fee_wnative.add(beefyTransferAmount);
          reportRow.total_strategist_fee_wnative = reportRow.total_strategist_fee_wnative.add(strategistTransferAmount);
          reportRow.total_caller_fee_wnative = reportRow.total_caller_fee_wnative.add(callerTransfer.value);
          reportRow.total_vault_compound_wnative = reportRow.total_vault_compound_wnative.add(compoundAmount);
        } else if (transferBatch.length === 0) {
          // first batch
          continue;
        } else {
          throw new Error(
            `[FR] Unexpected number of transfers in block ${currentBlockNumber} for ${chain}:${vault.id}:${strategy.implementation}`
          );
        }
      }
    }
    /*
    console.log({
      ...reportRow,
      total_strategist_fee_wnative: formatBigNumber(reportRow.total_strategist_fee_wnative, wnativeTokenDecimals),
      total_strategist_fee_usd: reportRow.total_strategist_fee_usd.toString(),
      total_beefy_fee_wnative: formatBigNumber(reportRow.total_beefy_fee_wnative, wnativeTokenDecimals),
      total_beefy_fee_usd: reportRow.total_beefy_fee_usd.toString(),
      total_caller_fee_wnative: formatBigNumber(reportRow.total_caller_fee_wnative, wnativeTokenDecimals),
      total_caller_fee_usd: reportRow.total_caller_fee_usd.toString(),
      total_vault_compound_wnative: formatBigNumber(reportRow.total_vault_compound_wnative, wnativeTokenDecimals),
      total_vault_compound_usd: reportRow.total_vault_compound_usd.toString(),
    });*/
  } catch (e) {
    logger.error(`[DCR] Error generating coverage report for ${chain}:${vault.id} : ${e}`);
    if (LOG_LEVEL === "trace") {
      console.log(e);
    }
  }
  return reportRow;
}

function formatBigNumber(value: ethers.BigNumber, decimals: number): string {
  const num = new BigNumber(value.toString());
  return num.shiftedBy(-decimals).toString(10);
}

runMain(main);
