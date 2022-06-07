import { logger } from "../utils/logger";
import { db_query, db_query_one } from "../utils/db";
import { BigNumber } from "bignumber.js";
import { InfluxDB, Point } from "@influxdata/influxdb-client";

// You can generate an API token from the "API Tokens Tab" in the UI
const token =
  "e9zlxVdCo1bvFsvzlPNlyYCSU_cyVKeOuFKFI9Xhwram2onrYircA6nFzYZzMBBBk0cdGBMlnD-QY2QeCXW85A==";
const org = "beefy";
const bucket = "beefy";

const client = new InfluxDB({
  url: "http://localhost:8086",
  token: token,
  timeout: 1000 * 60 * 5,
});
const writeApi = client.getWriteApi(org, bucket, "ms");

async function main() {
  const chain = "fantom";
  const mintBurnAddr = "0x0000000000000000000000000000000000000000";

  //const contractAddress = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  //const priceOracleId = "tomb-usdc-fusdt";
  const contractAddress = "0x41D44B276904561Ac51855159516FD4cB2c90968";
  const priceOracleId = "boo-ftm-usdc";
  const underlyingTokenDecimals = 18;
  const vaultTokenDecimals = 18;

  const transfersWithRateRows = await db_query<{
    time: Date;
    from: string;
    to: string;
    value: string;
    rate: string;
  }>(
    `
    SELECT
      erc20_transfer.time at time zone 'utc' as time,
      erc20_transfer.from_address as from,
      erc20_transfer.to_address as to,
      erc20_transfer.value,
      vault_token_to_underlying_rate.rate
    FROM erc20_transfer
      join vault_token_to_underlying_rate using (chain, contract_address, block_number)
    WHERE erc20_transfer.chain = %L AND erc20_transfer.contract_address = %L
    order by erc20_transfer.time asc;
  `,
    [chain, contractAddress]
  );

  const transfersWithRateTs = new ConsumableTs(transfersWithRateRows);

  const pricesRows = await db_query<{
    time: Date;
    price: number;
  }>(
    `
    SELECT
      time at time zone 'utc' as time,
      price
    FROM token_price_ts
    WHERE chain = %L AND contract_address = %L
    order by time asc;
  `,
    [chain, contractAddress]
  );

  const BIG_ZERO = new BigNumber(0);
  const currentBalance: { [accountAddr: string]: BigNumber } = {};
  let currentPPFS = new BigNumber(1); // ppfs cannot be interpreted as a decimal

  let totalPoints = 0;
  for (const [idx, priceRow] of pricesRows.entries()) {
    if (idx % 100 === 0) {
      logger.verbose(
        `Processing price row ${idx}/${pricesRows.length} (Points sent: ${totalPoints})`
      );
    }
    // compute balance and ppfs at the time of the price
    const transfers = transfersWithRateTs.consumeBeforeOrEqual(priceRow.time);
    for (const transfer of transfers) {
      // convert value to decimals
      const value = new BigNumber(transfer.value.toString()).shiftedBy(
        -vaultTokenDecimals
      );

      if (!currentBalance[transfer.from] && transfer.from !== mintBurnAddr) {
        currentBalance[transfer.from] = BIG_ZERO;
      }
      if (!currentBalance[transfer.to] && transfer.to !== mintBurnAddr) {
        currentBalance[transfer.to] = BIG_ZERO;
      }

      if (transfer.from !== mintBurnAddr) {
        currentBalance[transfer.from] =
          currentBalance[transfer.from].minus(value);
      }
      if (transfer.to !== mintBurnAddr) {
        currentBalance[transfer.to] = currentBalance[transfer.to].plus(value);
      }
    }

    // now, compute usd value for each user and send to db
    for (const [addr, balance] of Object.entries(currentBalance)) {
      const underlyingValue = mooAmountToOracleAmount(
        vaultTokenDecimals,
        underlyingTokenDecimals,
        currentPPFS,
        balance
      );
      const usdValue = underlyingValue.multipliedBy(priceRow.price);

      const point = new Point("account_investment_value")
        .tag("chain", chain)
        .tag("vault_addr", contractAddress)
        .tag("vault_id", priceOracleId)
        .tag("account_addr", addr)
        .floatField("usd_value", usdValue.toNumber())
        .timestamp(priceRow.time.getTime());
      //writeApi.writePoint(point);
      totalPoints++;
    }

    // cleanup zero balances after we pushed 0 to the api
    for (const addr of Object.keys(currentBalance)) {
      if (currentBalance[addr].isZero()) {
        delete currentBalance[addr];
      }
    }

    if (idx % 1000 === 0) {
      logger.verbose(`Flushing influxdb`);
      //await writeApi.flush();
      //await sleep(10_000);
      /*
      const usdBalance = Object.entries(currentBalance).reduce(
        (acc, [addr, balance]) => {
          const underlyingValue = mooAmountToOracleAmount(
            vaultTokenDecimals,
            underlyingTokenDecimals,
            currentPPFS,
            balance
          );
          const usdValue = underlyingValue.multipliedBy(priceRow.price);

          return Object.assign(acc, { [addr]: usdValue });
        },
        {} as { [addr: string]: BigNumber }
      );
      console.log(
        Object.entries(usdBalance).reduce((acc, [addr, usdValue]) =>
          Object.assign(acc, { [addr]: usdValue.toString(10) })
        )
      );

      const tvl = Object.entries(usdBalance).reduce(
        (acc, [_, usdValue]) => acc.plus(usdValue),
        BIG_ZERO
      );
      console.log({
        tvl: tvl.toString(10),
        accounts: Object.keys(usdBalance).length,
        small_accounts_10: Object.values(usdBalance).filter((balance) =>
          balance.isLessThan(10)
        ).length,
        small_accounts_50: Object.values(usdBalance).filter((balance) =>
          balance.isLessThan(50)
        ).length,
        small_accounts_100: Object.values(usdBalance).filter((balance) =>
          balance.isLessThan(100)
        ).length,
      });*/
    }
  }
  await writeApi.flush();
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

class ConsumableTs<TItem extends { time: Date }> {
  protected currentIdx = -1;
  protected length = 0;
  constructor(protected orderedEntries: TItem[]) {
    this.currentIdx = -1;
    this.length = orderedEntries.length;
  }

  public consumeBeforeOrEqual(time: Date) {
    const nextIdx = this.orderedEntries.findIndex(
      (entry, idx) =>
        idx > this.currentIdx && entry.time.getTime() >= time.getTime()
    );
    if (nextIdx === -1) {
      this.currentIdx = this.length - 1;
      return [];
    }
    const res = this.orderedEntries.slice(this.currentIdx + 1, nextIdx);
    this.currentIdx = nextIdx - 1;
    return res;
  }
}

class ConsumableAsyncTs<TItem extends { time: Date }> {
  protected done: boolean;
  protected unconsumedEntry: TItem | null;
  constructor(protected orderedStream: AsyncGenerator<TItem, void, unknown>) {
    this.unconsumedEntry = null;
    this.done = false;
  }

  public async consumeBeforeOrEqual(time: Date) {
    const res: TItem[] = [];
    if (this.done) {
      return [];
    }

    if (this.unconsumedEntry) {
      // next entry not consumable
      if (this.unconsumedEntry.time.getTime() > time.getTime()) {
        return [];
      }
      // otherwise, consume the unconsumed entry
      res.push(this.unconsumedEntry);
      this.unconsumedEntry = null;
    }
    let maxIter = 1_000_000;
    while (true || maxIter-- > 0) {
      const nextEntry = await this.orderedStream.next();
      if (nextEntry.done || !nextEntry.value) {
        this.done = true;
        return res;
      }
      if (nextEntry.value.time.getTime() > time.getTime()) {
        this.unconsumedEntry = nextEntry.value;
        return res;
      }
      res.push(nextEntry.value);
    }
  }
}

function generateTimeTs(min_date: Date, max_date: Date, snapshotDelta: number) {
  const min_time =
    Math.floor(min_date.getTime() / snapshotDelta) * snapshotDelta;
  const max_time =
    Math.ceil(max_date.getTime() / snapshotDelta) * snapshotDelta;
  return Array.from(
    {
      length: Math.ceil((max_time - min_time) / snapshotDelta),
    },
    (_, i) => new Date(min_time + i * snapshotDelta)
  );
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
