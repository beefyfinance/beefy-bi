import Decimal from "decimal.js";
import { ethers } from "ethers";
import * as fs from "fs";
import { groupBy, sortBy } from "lodash";
import * as path from "path";

Decimal.set({
  // make sure we have enough precision
  precision: 50,
  // configure the Decimals lib to format without exponents
  toExpNeg: -250,
  toExpPos: 250,
});

async function main() {
  const ppfs: { [key: string]: Decimal } = {
    bsc: new Decimal("1910063186902032800"),
    polygon: new Decimal("1202892791533687381"),
    optimism: new Decimal("1007904431569167265"),
  };
  const mooTokenDecimals = 18; // BeefyVaultV6
  const depositTokenDecimals = 18; // BIFI token
  const rates = {
    bsc: ppfsToVaultSharesRate(mooTokenDecimals, depositTokenDecimals, ppfs["bsc"]),
    polygon: ppfsToVaultSharesRate(mooTokenDecimals, depositTokenDecimals, ppfs["polygon"]),
    optimism: ppfsToVaultSharesRate(mooTokenDecimals, depositTokenDecimals, ppfs["optimism"]),
  };

  const boostContractAddresses = read_csv<{ contract_address: string }>("boost_address.csv").map((row) => normalizeAddress(row.contract_address));

  interface HolderRow {
    HolderAddress: string;
    Balance: string;
    PendingBalanceUpdate: string;
  }
  const formatRow = (row: HolderRow, chain: "bsc" | "polygon" | "optimism") => ({
    address: normalizeAddress(row.HolderAddress),
    mooBalance: new Decimal(row.Balance),
    bifiBalance: new Decimal(row.Balance).mul(rates[chain]),
    chain: chain,
  });

  // amount has decimals applied
  let holders = [
    ...read_csv<HolderRow>("bsc.csv").map((row) => formatRow(row, "bsc")),
    ...read_csv<HolderRow>("polygon.csv").map((row) => formatRow(row, "polygon")),
    ...read_csv<HolderRow>("optimism.csv").map((row) => formatRow(row, "optimism")),
  ];

  // all holders

  // remove boosts
  holders = holders.filter((holder) => !boostContractAddresses.includes(holder.address));

  // merge all holders chains
  let mergedHolders = Object.entries(groupBy(holders, "address")).map(([address, rows]) => ({
    address,
    bifiBalance: rows.reduce((agg, r) => agg.add(r.bifiBalance), new Decimal(0)),
    details: rows.map((row) => ({ chain: row.chain, mooBalance: row.mooBalance, bifiBalance: row.bifiBalance })),
  }));

  const threshold = new Decimal("0.1");
  const result = mergedHolders.map((row) => ({ ...row, details: JSON.stringify(row.details), matchThreshold: row.bifiBalance.gte(threshold) }));

  write_csv(
    "holders.csv",
    ["address", "bifiBalance", "matchThreshold", "details"],
    sortBy(
      result.filter((row) => row.matchThreshold),
      (row) => row.bifiBalance.mul(-1).toNumber(),
    ),
  );
}

function read_csv<T>(filename: string): T[] {
  const content = fs.readFileSync(path.join(__dirname, filename), "utf-8");
  const lines = content.split("\n");
  const headers: string[] = lines[0].split(",").map((header) => header.trim().replace(/^"(.*)"$/, "$1"));
  const data = lines.slice(1).map((line) => {
    const values: string[] = line.split(",").map((data) => data.trim().replace(/^"(.*)"$/, "$1"));
    return headers.reduce((acc, header, index) => {
      return { ...acc, [header]: values[index] };
    }, {});
  });
  return data as T[];
}

function write_csv<T>(filename: string, headers: (keyof T)[], data: T[]) {
  const content = [headers.join(",")].concat(data.map((row) => headers.map((header) => row[header]).join(","))).join("\n");
  fs.writeFileSync(path.join(__dirname, filename), content, "utf-8");
}

// takes ppfs and compute the actual rate which can be directly multiplied by the vault balance
// this is derived from mooAmountToOracleAmount in beefy-v2 repo
function ppfsToVaultSharesRate(mooTokenDecimals: number, depositTokenDecimals: number, ppfs: Decimal) {
  const mooTokenAmount = new Decimal("1.0");

  // go to chain representation
  const mooChainAmount = mooTokenAmount.mul(new Decimal(10).pow(mooTokenDecimals)).toDecimalPlaces(0);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.mul(ppfs);

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount.div(new Decimal(10).pow(mooTokenDecimals + depositTokenDecimals)).toDecimalPlaces(mooTokenDecimals);

  return oracleAmount;
}

function normalizeAddress(address: string) {
  // special case to avoid ethers.js throwing an error
  // Error: invalid address (argument="address", value=Uint8Array(0x0000000000000000000000000000000000000000), code=INVALID_ARGUMENT, version=address/5.6.1)
  if (address === "0x0000000000000000000000000000000000000000") {
    return address;
  }
  return ethers.utils.getAddress(address);
}

async function runMain(main: () => Promise<any>) {
  try {
    await main();
    console.log("Done");
    process.exit(0);
  } catch (e) {
    console.error("ERROR");
    console.error(e);
    process.exit(1);
  }
}

runMain(main);
