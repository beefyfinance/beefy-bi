import { chunk, flatten, groupBy, keyBy, maxBy } from "lodash";
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
import { BeefyVault } from "../types/beefy";
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
  strategies: BeefyVaultV6StrategiesData[] | null;
  transfer_from_count: Record<string, number>;
  harvest_count: Record<string, number>;
  start_date: Date;
  end_date: Date;
  total_strategist_fee_wnative: ethers.BigNumber;
  total_beefy_fee_wnative: ethers.BigNumber;
  total_caller_fee_wnative: ethers.BigNumber;
  total_vault_compound_wnative: ethers.BigNumber;
  total_ukn_transfers_wnative: ethers.BigNumber;
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
      total_beefy_fee_wnative: formatBigNumber(reportRow.total_beefy_fee_wnative, reportRow.wnative_decimals),
      total_caller_fee_wnative: formatBigNumber(reportRow.total_caller_fee_wnative, reportRow.wnative_decimals),
      total_vault_compound_wnative: formatBigNumber(reportRow.total_vault_compound_wnative, reportRow.wnative_decimals),
      total_ukn_fee_wnative: formatBigNumber(reportRow.total_ukn_transfers_wnative, reportRow.wnative_decimals),
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
    harvest_count: {},
    start_date: new Date(),
    end_date: new Date(0),
    total_strategist_fee_wnative: ethers.BigNumber.from(0),
    total_beefy_fee_wnative: ethers.BigNumber.from(0),
    total_caller_fee_wnative: ethers.BigNumber.from(0),
    total_vault_compound_wnative: ethers.BigNumber.from(0),
    total_ukn_transfers_wnative: ethers.BigNumber.from(0),
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
      const feeRecipientsData = await feeRecipientsStore.getLocalData(chain, strategy.implementation);
      if (!feeRecipientsData) {
        logger.debug(`[FR] No fee recipients found for ${strategy.implementation}`);
        continue;
      }
      for (const feeRecipients of feeRecipientsData.recipientsAtBlock) {
        if (feeRecipients.beefyFeeRecipient) {
          addressRoleMap[normalizeAddress(feeRecipients.beefyFeeRecipient)] = "beefy";
        }
        addressRoleMap[normalizeAddress(feeRecipients.strategist)] = "strategist";
      }
    }

    logger.verbose(`[FR] addressRoleMap for ${chain}:${vault.id}: ${JSON.stringify(addressRoleMap)}`);
    const roleAddressMap: Record<"beefy" | "strategist", string[]> = { beefy: [], strategist: [] };
    for (const [address, role] of Object.entries(addressRoleMap)) {
      roleAddressMap[role].push(address);
    }

    let isMaxiVault = vault.id.endsWith("bifi-maxi");
    let strategyHarvestTransferCount: null | number = null;
    for (const strategy of strategies) {
      reportRow.transfer_from_count[strategy.implementation] = 0;
      reportRow.harvest_count[strategy.implementation] = 0;

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

        // skip the first empty batch
        if (transferBatch.length === 0) {
          continue;
        }

        // now we have identified our first batch, remember his size
        // this is how much transfers per harvest there should be
        if (strategyHarvestTransferCount === null) {
          strategyHarvestTransferCount = transferBatch.length;
          if (
            !(
              strategyHarvestTransferCount % 4 === 0 ||
              strategyHarvestTransferCount % 3 === 0 ||
              (isMaxiVault && strategyHarvestTransferCount === 2)
            )
          ) {
            logger.error(
              `[FR] Invalid strategy harvest transfer count for ${chain}:${vault.id}:${contractAddress}:${strategy.implementation}: ${strategyHarvestTransferCount}`
            );
            continue;
          }
        }

        // then, process the batch, there could be multiple harvests is a single block
        if (transferBatch.length % strategyHarvestTransferCount !== 0) {
          const message = `[FR] Unexpected number of transfers (${transferBatch.length}) in block ${currentBlockNumber}, expecting a multiple of ${strategyHarvestTransferCount} for ${chain}:${vault.id}:${strategy.implementation}`;
          // if we have more transfers than needed, parse anyway
          if (transferBatch.length < strategyHarvestTransferCount) {
            logger.error(message);
            continue;
          } else {
            logger.debug(message);
          }
        }

        const transferPerHarvest = chunk(transferBatch, strategyHarvestTransferCount);

        for (const harvest of transferPerHarvest) {
          reportRow.harvest_count[strategy.implementation] += 1;

          // if we have 4 transfers exactly, it's easy
          // there should be 2 addresses in the address map (strategist and beefy)
          // there should be 1 transfer larger than the other ones, it's the compound
          // the last one is the caller
          // with 3 transfers, there is no wnative compound but the logic is the same
          // with 2 transfers (maxi vault), there is no treasury transfer (only compound and caller)
          let cleanTransferBatch: TransferToValue[] = harvest.map((transfer) => ({
            to: normalizeAddress(transfer.to),
            value: ethers.BigNumber.from(transfer.value),
          }));
          let beefyTransfer: null | TransferToValue = null;
          let strategistTransfer: null | TransferToValue = null;
          let compoundTransfer: null | TransferToValue = null;
          let callerTransfer: null | TransferToValue = null;
          let unknownTargetTotal: ethers.BigNumber = ethers.BigNumber.from(0);

          // identify the easy ones
          [beefyTransfer, cleanTransferBatch] = findLastAndConsume(cleanTransferBatch, (t) =>
            roleAddressMap["beefy"].includes(t.to)
          );
          if ((strategyHarvestTransferCount === 4 || strategyHarvestTransferCount === 3) && !beefyTransfer) {
            logger.debug(
              `[FR] No beefy treasury transfer found for ${chain}:${strategy.implementation} on block ${currentBlockNumber}`
            );
          }

          // we need to identify the strategist transfer idx for later
          [strategistTransfer, cleanTransferBatch] = findLastAndConsume(cleanTransferBatch, (t) =>
            roleAddressMap["strategist"].includes(t.to)
          );
          if (!strategistTransfer && !isMaxiVault) {
            logger.debug(
              `[FR] No strategist transfer found for ${chain}:${strategy.implementation} on block ${currentBlockNumber}`
            );
          }

          // find the biggest transfer if we have 4 transfers, otherwise there is no compound
          if (harvest.length >= 4) {
            const maxValue = maxBy(cleanTransferBatch, (t) => t.value);
            [compoundTransfer, cleanTransferBatch] = findFirstAndConsume(cleanTransferBatch, (t) => t === maxValue);

            // but if there is only trx, it's a compound
          } else if (harvest.length === 1) {
            [compoundTransfer, cleanTransferBatch] = findFirstAndConsume(cleanTransferBatch, () => true);
          }

          // last one in the batch should be the caller
          if (cleanTransferBatch.length === 1) {
            [callerTransfer, cleanTransferBatch] = findFirstAndConsume(cleanTransferBatch, () => true);
          }

          // some data wasn't attributed, we still count it
          if (cleanTransferBatch.length > 0) {
            for (const transfer of cleanTransferBatch) {
              unknownTargetTotal = unknownTargetTotal.add(transfer.value);
            }
          }
          /*
          console.log({
            blockNumber: harvest[0].blockNumber,
            beefyTransfer: beefyTransfer?.value.toString(),
            strategistTransfer: strategistTransfer?.value.toString(),
            compoundTransfer: compoundTransfer?.value.toString(),
            callerTransfer: callerTransfer?.value.toString(),
          });*/
          reportRow.total_beefy_fee_wnative = reportRow.total_beefy_fee_wnative.add(
            beefyTransfer ? beefyTransfer.value : ethers.BigNumber.from(0)
          );
          reportRow.total_strategist_fee_wnative = reportRow.total_strategist_fee_wnative.add(
            strategistTransfer ? strategistTransfer.value : ethers.BigNumber.from(0)
          );
          reportRow.total_caller_fee_wnative = reportRow.total_caller_fee_wnative.add(
            callerTransfer ? callerTransfer.value : ethers.BigNumber.from(0)
          );
          reportRow.total_vault_compound_wnative = reportRow.total_vault_compound_wnative.add(
            compoundTransfer ? compoundTransfer.value : ethers.BigNumber.from(0)
          );
          reportRow.total_ukn_transfers_wnative = reportRow.total_ukn_transfers_wnative.add(unknownTargetTotal);
        }
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

interface TransferToValue {
  to: string;
  value: ethers.BigNumber;
}

function findFirstAndConsume(
  transfers: TransferToValue[],
  condition: (transfer: TransferToValue) => boolean
): [TransferToValue | null, TransferToValue[]] {
  const first = transfers.find(condition);
  if (first) {
    const rest = transfers.filter((t) => t !== first);
    return [first, rest];
  }
  return [null, transfers];
}

function findLastAndConsume(
  transfers: TransferToValue[],
  condition: (transfer: TransferToValue) => boolean
): [TransferToValue | null, TransferToValue[]] {
  let [value, rest] = findFirstAndConsume(transfers.reverse(), condition);
  return [value, rest.reverse()];
}

function formatBigNumber(value: ethers.BigNumber, decimals: number): string {
  const num = new BigNumber(value.toString());
  return num.shiftedBy(-decimals).toString(10);
}

runMain(main);
