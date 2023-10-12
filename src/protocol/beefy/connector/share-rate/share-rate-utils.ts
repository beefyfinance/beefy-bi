import Decimal from "decimal.js";
import { ethers } from "ethers";
import { get, isArray } from "lodash";
import { Chain } from "../../../../types/chain";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, isInRange } from "../../../../utils/range";
import { DbBeefyStdVaultProduct } from "../../../common/loader/product";
import { ImportCtx } from "../../../common/types/import-context";

export interface BeefyShareRateCallParams {
  chain: Chain;
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
