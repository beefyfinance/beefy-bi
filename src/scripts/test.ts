import { logger } from "../utils/logger";
import _ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import BigNumber from "bignumber.js";
import { getAllVaultsFromGitHistory } from "../lib/git-get-all-vaults";
import { allChainIds, Chain } from "../types/chain";
import prettier from "prettier";
import * as fs from "fs";

async function main() {
  const result: { chain: Chain; count: number }[] = [];
  for (const chain of allChainIds) {
    const vaults = await getAllVaultsFromGitHistory(chain);
    result.push({ chain, count: vaults.length });
  }
  console.log(result.map((r) => `${r.chain}:${r.count}`).join("\n"));
  /*
  const data = {
    chain: "fantom",
    vault_id: "stargate-fantom-usdc",
    investor_address: "0x0DC66F1Dcb64D68F77895a44B023988b036b55dA",
    datetime: "2022-04-03T12:00:00Z",
    moo_amount_balance: "5792846432",
    //         0.000000005792846432
    ppfs: "1001654706118418760",
    //     1001684972421921375
    want_decimals: "6",
    want_usd_value: "1.00002199510531150625",
    investor_want_amount: "5802431890434090.68334786432",
    investor_usd_amount: "5802559515534583.623467093612488173784259832",
  };

  const mooAmount = new BigNumber("5792846432");
  const ppfs = new BigNumber("1001684972421921375");
  const wantDecimals = 6;
  const wantUsdValue = new BigNumber("1.00002199510531150625");

  const oracleAmount = mooAmount.multipliedBy(ppfs).shiftedBy(-(18 + 6));
  console.log(oracleAmount.toString());
  const oracleAmount2 = mooAmount.shiftedBy(-(18 + 6)).multipliedBy(ppfs);
  console.log(oracleAmount2.toString());
*/
  /*

  const wantAmount = mooAmountToOracleAmount(
    18,
    18,
    new BigNumber(data.ppfs),
    new BigNumber(data.moo_amount_balance).shiftedBy(-18)
  );
  console.log(wantAmount.toString(10));
  const wantUsdValue = new BigNumber(data.want_usd_value);
  const usdAmount = wantUsdValue.multipliedBy(wantAmount);
  console.log(usdAmount.toString(10));*/
}

function mooAmountToOracleAmount(
  mooTokenDecimals: number,
  depositTokenDecimals: number,
  ppfs: BigNumber,
  mooTokenAmount: BigNumber
) {
  // go to chain representation
  const mooChainAmount = mooTokenAmount.shiftedBy(mooTokenDecimals);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.multipliedBy(ppfs);

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount
    .shiftedBy(-depositTokenDecimals)
    .decimalPlaces(depositTokenDecimals);

  return oracleAmount;
}

main()
  .then(() => {
    logger.info("Done");
    process.exit(0);
  })
  .catch((e) => {
    console.log(e);
    logger.error(e);
    process.exit(1);
  });
