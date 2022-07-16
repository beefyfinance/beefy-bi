import { ethers } from "ethers";
import { chunk, maxBy } from "lodash";
import { BeefyVault } from "../../types/beefy";
import { Chain } from "../../types/chain";
import { normalizeAddress } from "../../utils/ethers";
import { logger } from "../../utils/logger";
import { ERC20TransferFromEventData } from "../csv-store/csv-transfer-from-events";

export interface FeeReportRow {
  datetime: Date;
  harvest_count: number;
  caller_wnative_amount: ethers.BigNumber;
  strategist_wnative_amount: ethers.BigNumber;
  beefy_wnative_amount: ethers.BigNumber;
  compound_wnative_amount: ethers.BigNumber;
  ukn_wnative_amount: ethers.BigNumber;
}

interface TransferToValue {
  to: string;
  value: ethers.BigNumber;
}

const BIG_ZERO = ethers.BigNumber.from(0);

export function transferBatchToFeeReports(
  chain: Chain,
  vault: BeefyVault,
  strategy: string,
  strategyHarvestTransferCount: number,
  roleAddressMap: { [role: string]: string[] },
  transferBatch: ERC20TransferFromEventData[]
): FeeReportRow[] {
  const isMaxiVault = vault.id.endsWith("-bifi-maxi");
  const transferPerHarvest = chunk(transferBatch, strategyHarvestTransferCount);
  const feeReports: FeeReportRow[] = [];
  const currentBlockNumber = transferBatch[0].blockNumber;
  const currentDatetime = transferBatch[0].datetime;

  // then, process the batch, there could be multiple harvests is a single block
  if (transferBatch.length % strategyHarvestTransferCount !== 0) {
    logger.debug(
      `[FR] Unexpected number of transfers (${transferBatch.length}) in block ${currentBlockNumber}, expecting a multiple of ${strategyHarvestTransferCount} for ${chain}:${vault.id}:${strategy}`
    );
  }

  for (const harvest of transferPerHarvest) {
    const feeReportRow = {
      datetime: currentDatetime,
      harvest_count: 1,
      caller_wnative_amount: BIG_ZERO,
      strategist_wnative_amount: BIG_ZERO,
      beefy_wnative_amount: BIG_ZERO,
      compound_wnative_amount: BIG_ZERO,
      ukn_wnative_amount: BIG_ZERO,
    };

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
      logger.debug(`[FR] No beefy treasury transfer found for ${chain}:${strategy} on block ${currentBlockNumber}`);
    }

    // we need to identify the strategist transfer idx for later
    [strategistTransfer, cleanTransferBatch] = findLastAndConsume(cleanTransferBatch, (t) =>
      roleAddressMap["strategist"].includes(t.to)
    );
    if (!strategistTransfer && !isMaxiVault) {
      logger.debug(`[FR] No strategist transfer found for ${chain}:${strategy} on block ${currentBlockNumber}`);
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
    feeReportRow.beefy_wnative_amount = beefyTransfer?.value ?? BIG_ZERO;
    feeReportRow.strategist_wnative_amount = strategistTransfer?.value ?? BIG_ZERO;
    feeReportRow.compound_wnative_amount = compoundTransfer?.value ?? BIG_ZERO;
    feeReportRow.caller_wnative_amount = callerTransfer?.value ?? BIG_ZERO;
    feeReportRow.ukn_wnative_amount = unknownTargetTotal;

    feeReports.push(feeReportRow);
  }
  return feeReports;
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
