import Decimal from "decimal.js";
import { ethers } from "ethers";
import { get, isArray } from "lodash";
import { BeefyVaultV6AbiInterface } from "../../../../utils/abi";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, isInRange } from "../../../../utils/range";
import { DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ImportCtx } from "../../../common/types/import-context";

export interface BeefyShareRateCallParams {
  vaultDecimals: number;
  underlyingDecimals: number;
  vaultAddress: string;
  blockNumber: number;
}

export interface BeefyShareRateBatchCallResult {
  shareRate: Decimal;
  blockNumber: number;
  blockDatetime: Date;
}

export const getBlockTag = (ctx: ImportCtx, blockNumber: number): number => {
  // read the next block for those chains who can't read their own writes
  let blockTag = blockNumber;
  if (!ctx.rpcConfig.rpcLimitations.stateChangeReadsOnSameBlock) {
    blockTag = blockNumber + 1;
  }
  return blockTag;
};
export function getCallParamsFromProductAndRange(product: DbBeefyStdVaultProduct, range: Range<number>): BeefyShareRateCallParams {
  const vault = product.productData.vault;
  return {
    underlyingDecimals: vault.want_decimals,
    vaultAddress: vault.contract_address,
    vaultDecimals: vault.token_decimals,
    blockNumber: rangeToBlockNumber(range),
  };
}

export function rangeToBlockNumber(range: Range<number>): number {
  const midPoint = Math.round((range.from + range.to) / 2);
  if (!isInRange(range, midPoint)) {
    throw new ProgrammerError({ msg: "Midpoint is not in range, most likely an invalid range", data: { range, midPoint } });
  }
  return midPoint;
}

// sometimes, we get this error: "execution reverted: SafeMath: division by zero"
// this means that the totalSupply is 0 so we set ppfs to zero
export function isEmptyVaultPPFSError(err: any) {
  if (!err) {
    return false;
  }
  const errorMessage = get(err, ["error", "message"]) || get(err, "message") || "";
  return errorMessage.includes("SafeMath: division by zero");
}

// a call to an address without code (before the vault is deployed, or a wrong address) does not revert:
// a multicall sub-call succeeds with empty return data, and a direct call reverts with empty data
// in that case there is no ppfs, so we use 0 (same as an empty vault)
export function isNoCodeReturnData(returnData: string): boolean {
  return returnData === "0x";
}

export function isEmptyReturnDataCallError(err: any): boolean {
  return get(err, "code") === "CALL_EXCEPTION" && (get(err, "data") === "0x" || get(err, ["error", "data"]) === "0x");
}

export async function hasNoCodeAt(provider: ethers.providers.Provider, address: string, blockTag: number): Promise<boolean> {
  return isNoCodeReturnData(await provider.getCode(address, blockTag));
}

/**
 * Decodes the ppfs of a multicall sub-call (allowFailure: true)
 * returns null if the call failed (the caller decides the default), 0 if there is no code, the ppfs otherwise
 */
export function decodePpfsMulticallResult(ppfsData: { success: boolean; returnData: string }): ethers.BigNumber | null {
  if (!ppfsData.success) {
    return null;
  }
  if (isNoCodeReturnData(ppfsData.returnData)) {
    return ethers.BigNumber.from(0);
  }
  return extractRawPpfsFromFunctionResult(
    BeefyVaultV6AbiInterface.decodeFunctionResult("getPricePerFullShare", ppfsData.returnData),
  ) as ethers.BigNumber;
}

export function extractRawPpfsFromFunctionResult<T>(returnData: [T] | T): T {
  // some chains don't return an array (harmony, heco)
  return isArray(returnData) ? returnData[0] : returnData;
}

// takes ppfs and compute the actual rate which can be directly multiplied by the vault balance
// this is derived from mooAmountToOracleAmount in beefy-v2 repo
export function ppfsToVaultSharesRate(mooTokenDecimals: number, depositTokenDecimals: number, ppfs: ethers.BigNumber) {
  const mooTokenAmount = new Decimal("1.0");

  // go to chain representation
  const mooChainAmount = mooTokenAmount.mul(new Decimal(10).pow(mooTokenDecimals)).toDecimalPlaces(0);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.mul(new Decimal(ppfs.toString()));

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount.div(new Decimal(10).pow(mooTokenDecimals + depositTokenDecimals)).toDecimalPlaces(mooTokenDecimals);

  return oracleAmount;
}
