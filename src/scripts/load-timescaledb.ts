import { from as copyFrom } from "pg-copy-streams";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { allChainIds } from "../types/chain";
import { transform } from "stream-transform";
import { Chain } from "../types/chain";
import { stringify } from "csv-stringify";
import { FeeReportRow, transferBatchToFeeReports } from "../lib/beefy/transfers-to-harvest";
import {
  db_query,
  db_query_one,
  getPgPool,
  rebuildVaultStatsReportTable,
  strAddressToPgBytea,
  strArrToPgStrArr,
} from "../utils/db";
import { logger } from "../utils/logger";
import { ERC20EventData, erc20TransferStore } from "../lib/csv-store/csv-transfer-events";
import {
  FlattenStream,
  StreamAggBy,
  StreamBatchBy,
  StreamConsoleLogDebug,
  StreamFilterTransform,
} from "../utils/stream";
import { sleep } from "../utils/async";
import { BeefyVaultV6PPFSData, ppfsStore } from "../lib/csv-store/csv-vault-ppfs";
import { Transform } from "stream";
import { OraclePriceData, oraclePriceStore } from "../lib/csv-store/csv-oracle-price";
import { SamplingPeriod } from "../types/sampling";
import { ethers } from "ethers";
import { normalizeAddress } from "../utils/ethers";
import { vaultListStore } from "../beefy/connector/vault-list";
import { BeefyVault } from "../types/beefy";
import { LOG_LEVEL } from "../utils/config";
import { vaultStrategyStore } from "../lib/csv-store/csv-vault-strategy";
import { ERC20TransferFromEventData, erc20TransferFromStore } from "../lib/csv-store/csv-transfer-from-events";
import { getChainWNativeTokenAddress, getChainWNativeTokenDecimals } from "../utils/addressbook";
import { feeRecipientsStore } from "../lib/beefy/fee-recipients";
import { sumBy } from "lodash";
import BigNumber from "bignumber.js";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: false, alias: "v" },
      importOnly: {
        choices: [
          "erc20_transfers",
          "ppfs",
          "prices",
          "vaults",
          "refresh_materialized_views",
          "refresh_vault_stats_view",
          "fee_reports",
        ],
        alias: "o",
        demand: false,
      },
    }).argv;
  type ImportOnly =
    | "erc20_transfers"
    | "ppfs"
    | "prices"
    | "vaults"
    | "refresh_materialized_views"
    | "refresh_vault_stats_view"
    | "fee_reports";
  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId || null;
  const importOnly: ImportOnly | null = (argv.importOnly as ImportOnly) || null;

  if (!importOnly || importOnly === "fee_reports") {
    logger.info(`[LTSDB] Importing fee reports`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing fee reports for ${chain}`);
      const vaults = await vaultListStore.getLocalData(chain);
      for (const vault of vaults) {
        if (vaultId && vault.id !== vaultId) {
          logger.verbose(`[LTSDB] Skipping fee reports for ${chain}:${vault.id}`);
          continue;
        }
        try {
          await importFeeReportsToDb(chain, vault);
        } catch (err) {
          logger.error(`[LTSDB] Skipping fee reports for ${chain}:${vault.id}`);
          console.log(err);
        }
      }
    }
  }

  if (!importOnly || importOnly === "erc20_transfers") {
    logger.info(`[LTSDB] Importing ERC20 transfers`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing ERC20 transfers for ${chain}`);
      const vaults = await vaultListStore.getLocalData(chain);
      for (const vault of vaults) {
        if (vaultId && vault.id !== vaultId) {
          logger.verbose(`[LTSDB] Skipping ERC20 transfers for ${chain}:${vault.id}`);
          continue;
        }
        try {
          await importVaultERC20TransfersToDB(chain, vault);
        } catch (err) {
          logger.error(`[LTSDB] Skipping ERC20 transfers for ${chain}:${vault.id}`);
          console.log(err);
        }
      }
    }
  }

  if (!importOnly || importOnly === "ppfs") {
    logger.info(`[LTSDB] Importing ppfs`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing ppfs for ${chain}`);
      const vaults = await vaultListStore.getLocalData(chain);
      for (const vault of vaults) {
        if (vaultId && vault.id !== vaultId) {
          logger.verbose(`[LTSDB] Skipping ppfs for ${chain}:${vault.id}`);
          continue;
        }
        try {
          await importVaultPPFSToDB(chain, vault);
        } catch (err) {
          logger.error(`[LTSDB] Skipping ppfs for vault ${chain}:${vault.id}. ${err}`);
          if (LOG_LEVEL === "trace") {
            console.log(err);
          }
        }
      }
    }
  }

  if (!importOnly || importOnly === "prices") {
    logger.info(`[LTSDB] Importing prices`);

    const oracleIds = oraclePriceStore.getAllAvailableOracleIds("15min");
    // get the latest import date for all oracles in one go
    const res = await db_query<{ oracle_id: string; last_imported: Date }>(
      `SELECT oracle_id, max(datetime) as last_imported
      FROM data_raw.oracle_price_ts
      group by 1`
    );
    const lastImportedOraclePrices = res.reduce(
      (agg, cur) => Object.assign(agg, { [cur.oracle_id]: cur.last_imported }),
      {} as Record<string, Date>
    );

    for await (const oracleId of oracleIds) {
      await importPricesToDB(oracleId, lastImportedOraclePrices);
    }
  }

  if (!importOnly || importOnly === "vaults") {
    logger.info(`[LTSDB] Importing vaults`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing vaults for ${chain}`);
      const vaults = await vaultListStore.getLocalData(chain);
      if (vaults.length <= 0) {
        logger.verbose(`[LTSDB] No vaults found for ${chain}`);
        continue;
      }
      await db_query(
        `
        INSERT INTO data_raw.vault (
          chain,
          token_address,
          vault_id,
          token_name,
          want_address,
          want_decimals,
          want_price_oracle_id,
          end_of_life,
          assets_oracle_id
        ) values %L
        ON CONFLICT (chain, token_address) DO UPDATE SET 
          vault_id = data_raw.vault.vault_id,
          token_name = data_raw.vault.token_name,
          want_address = data_raw.vault.want_address,
          want_decimals = data_raw.vault.want_decimals,
          want_price_oracle_id = data_raw.vault.want_price_oracle_id,
          end_of_life = data_raw.vault.end_of_life,
          assets_oracle_id = data_raw.vault.assets_oracle_id
       ;
      `,
        [
          vaults.map((v) => [
            chain,
            strAddressToPgBytea(v.token_address),
            v.id,
            v.token_name,
            strAddressToPgBytea(v.want_address),
            v.want_decimals,
            v.price_oracle.want_oracleId,
            false, // end of life (eol)
            strArrToPgStrArr(v.price_oracle.assets),
          ]),
        ]
      );
    }
  }

  if (!importOnly || importOnly === "refresh_materialized_views") {
    logger.info(`[LTSDB] Refreshing materialized view: data_derived.vault_ppfs_and_price_4h_ts`);
    await db_query(`REFRESH MATERIALIZED VIEW data_derived.vault_ppfs_and_price_4h_ts`);
  }

  if (!importOnly || importOnly === "refresh_vault_stats_view") {
    logger.info(`[LTSDB] Refreshing vault stats`);
    await rebuildVaultStatsReportTable();
  }

  logger.info("[LTSDB] Finished importing data. Sleeping 4h...");
  await sleep(4 * 60 * 60 * 1000);
}

async function importVaultERC20TransfersToDB(chain: Chain, vault: BeefyVault) {
  const contractAddress = vault.token_address;

  await loadCSVStreamToTimescaleTable({
    logKey: `ERC20 Transfers diffs for ${chain}:${vault.id}`,

    dbCopyQuery: `COPY data_raw.erc20_balance_diff_ts (
        chain, contract_address, datetime, owner_address, 
        balance_diff, balance_before, balance_after
        ) FROM STDIN WITH CSV DELIMITER ',';`,

    getFileStreamAfterDate: async (date) =>
      date
        ? erc20TransferStore.getReadStreamAfterDate(date, chain, contractAddress)
        : erc20TransferStore.getReadStream(chain, contractAddress),

    getLastDbRowDate: async () =>
      (
        await db_query_one<{ last_imported: Date }>(
          `SELECT max(datetime) as last_imported
          FROM data_raw.erc20_balance_diff_ts
          WHERE chain = %L
            AND contract_address = %L`,
          [chain, strAddressToPgBytea(contractAddress)]
        )
      )?.last_imported || null,

    getLastFileDate: async () => (await erc20TransferStore.getLastRow(chain, contractAddress))?.datetime || null,

    rowToDbTransformer: await (async () => {
      // get latest balance from db per owner to propate it
      const rows = await db_query<{
        owner_address: string;
        balance_after: string;
      }>(
        `SELECT format_evm_address(owner_address) as owner_address, 
          last(balance_after, datetime) as balance_after
        from data_raw.erc20_balance_diff_ts
        where chain = %L
          and contract_address = %L
        group by owner_address`,
        [chain, strAddressToPgBytea(contractAddress)]
      );
      const lastBalancePerOwner = rows.reduce(
        (agg, row) =>
          Object.assign(agg, {
            [normalizeAddress(row.owner_address)]: ethers.BigNumber.from(row.balance_after),
          }),
        {} as Record<string, ethers.BigNumber>
      );
      const bigZero = ethers.BigNumber.from(0);

      return (data: ERC20EventData) => {
        if (data.from === data.to) {
          logger.verbose(`Ignoring self transfer from ${data.from} at block ${data.blockNumber}`);
        }
        const newDiffRows = [
          { owner: data.from, balance_diff: "-" + data.value },
          { owner: data.to, balance_diff: data.value },
        ].map((cfg) => {
          const ownerAddress = normalizeAddress(cfg.owner);
          const lastBalance = lastBalancePerOwner[ownerAddress] || bigZero;
          const balanceDiff = ethers.BigNumber.from(cfg.balance_diff);
          const newBalance = lastBalance.add(balanceDiff);
          // add a test to avoid inserting garbage
          if (newBalance.lt(0) && ownerAddress !== "0x0000000000000000000000000000000000000000") {
            throw new InconsistentUserBalance(chain, contractAddress, lastBalance, newBalance, data);
          }

          lastBalancePerOwner[ownerAddress] = newBalance;
          return [
            chain,
            strAddressToPgBytea(contractAddress),
            data.datetime.toISOString(),
            strAddressToPgBytea(ownerAddress),
            cfg.balance_diff, // diff
            lastBalance.toString(), // balance before
            newBalance.toString(), // balance after
          ];
        });
        return newDiffRows;
      };
    })(),
    flatten: true,
  });
}

async function importVaultPPFSToDB(chain: Chain, vault: BeefyVault) {
  const samplingPeriod = "4hour";
  const contractAddress = vault.token_address;

  return loadCSVStreamToTimescaleTable({
    logKey: `PPFS for ${chain}:${vault.id}`,

    dbCopyQuery: `COPY data_raw.vault_ppfs_ts (chain, contract_address, datetime, ppfs) FROM STDIN WITH CSV DELIMITER ',';`,

    getFileStreamAfterDate: async (date) =>
      date
        ? ppfsStore.getReadStreamAfterDate(date, chain, contractAddress, samplingPeriod)
        : ppfsStore.getReadStream(chain, contractAddress, samplingPeriod),

    getLastDbRowDate: async () =>
      (
        await db_query_one<{ last_imported: Date }>(
          `SELECT max(datetime) as last_imported
          FROM data_raw.vault_ppfs_ts
          WHERE chain = %L
            AND contract_address = %L`,
          [chain, strAddressToPgBytea(contractAddress)]
        )
      )?.last_imported || null,

    getLastFileDate: async () => (await ppfsStore.getLastRow(chain, contractAddress, samplingPeriod))?.datetime || null,

    rowToDbTransformer: (data: BeefyVaultV6PPFSData) => {
      return [
        // these should match the order of the copy cmd
        chain,
        strAddressToPgBytea(contractAddress),
        data.datetime.toISOString(),
        data.pricePerFullShare,
      ];
    },
    flatten: false,
  });
}

async function importPricesToDB(oracleId: string, lastImportedOraclePrices: Record<string, Date>) {
  const samplingPeriod: SamplingPeriod = "15min";

  return loadCSVStreamToTimescaleTable({
    logKey: `prices for ${oracleId}`,
    dbCopyQuery: `COPY data_raw.oracle_price_ts(oracle_id, datetime, usd_value) FROM STDIN WITH CSV DELIMITER ',';`,
    getFileStreamAfterDate: async (date) =>
      date
        ? oraclePriceStore.getReadStreamAfterDate(date, oracleId, samplingPeriod)
        : oraclePriceStore.getReadStream(oracleId, samplingPeriod),
    getLastDbRowDate: async () => lastImportedOraclePrices[oracleId] || null,
    getLastFileDate: async () => (await oraclePriceStore.getLastRow(oracleId, samplingPeriod))?.datetime || null,
    rowToDbTransformer: (data: OraclePriceData) => {
      return [
        // these should match the order of the copy cmd
        oracleId,
        data.datetime.toISOString(),
        data.usdValue,
      ];
    },
    flatten: false,
  });
}

async function importFeeReportsToDb(chain: Chain, vault: BeefyVault) {
  const strategies = vaultStrategyStore.getReadIterator(chain, vault.token_address);
  const wnativeTokenAddress = getChainWNativeTokenAddress(chain);
  const wnativeTokenDecimals = getChainWNativeTokenDecimals(chain);

  function formatBigNumber(value: ethers.BigNumber, decimals: number): string {
    const num = new BigNumber(value.toString());
    return num.shiftedBy(-decimals).toString(10);
  }

  for await (const strategy of strategies) {
    logger.verbose(`[FeeReport] Importing fee reports for ${chain}/${vault.id}/${strategy.implementation}`);
    const feeRecipientsData = await feeRecipientsStore.getLocalData(chain, strategy.implementation);
    if (!feeRecipientsData) {
      logger.debug(`[FR] No fee recipients found for ${strategy.implementation}`);
      continue;
    }
    const addressRoleMap: Record<string, "beefy" | "strategist"> = {};
    for (const feeRecipients of feeRecipientsData.recipientsAtBlock) {
      if (feeRecipients.beefyFeeRecipient) {
        addressRoleMap[normalizeAddress(feeRecipients.beefyFeeRecipient)] = "beefy";
      }
      addressRoleMap[normalizeAddress(feeRecipients.strategist)] = "strategist";
    }
    logger.verbose(`[FR] addressRoleMap for ${chain}:${vault.id}: ${JSON.stringify(addressRoleMap)}`);
    const roleAddressMap: Record<"beefy" | "strategist", string[]> = { beefy: [], strategist: [] };
    for (const [address, role] of Object.entries(addressRoleMap)) {
      roleAddressMap[role].push(address);
    }

    const lastImportedDate =
      (
        await db_query_one<{ last_imported: Date }>(
          `SELECT max(datetime) as last_imported
      FROM data_raw.vault_harvest_1d_ts
      WHERE chain = %L
        AND vault_id = %L
        and strategy_address = %L`,
          [chain, vault.id, strAddressToPgBytea(strategy.implementation)]
        )
      )?.last_imported || null;

    function dateTruncToDay(date: Date) {
      return new Date(date.toISOString().substring(0, 10) + "T00:00:00.000Z");
    }

    await loadCSVStreamToTimescaleTable({
      logKey: `harvests for ${chain}:${vault.id}`,
      dbCopyQuery: `COPY data_raw.vault_harvest_1d_ts(
        chain, vault_id, datetime, strategy_address, harvest_count,
        caller_wnative_amount, strategist_wnative_amount, beefy_wnative_amount, compound_wnative_amount, ukn_wnative_amount
      ) FROM STDIN WITH CSV DELIMITER ',';`,
      getFileStreamAfterDate: async (date) => {
        const fileStream = date
          ? erc20TransferFromStore.getReadStreamAfterDate(date, chain, strategy.implementation, wnativeTokenAddress)
          : erc20TransferFromStore.getReadStream(chain, strategy.implementation, wnativeTokenAddress);

        if (!fileStream) {
          return null;
        }
        let strategyHarvestCount: number | null = null;
        const BIG_ZER0 = ethers.BigNumber.from(0);
        return (
          fileStream
            // don't bother processing if we've already imported this date
            .pipe(
              new StreamFilterTransform<ERC20TransferFromEventData>((row) => {
                if (!lastImportedDate) {
                  return true;
                }
                return dateTruncToDay(row.datetime) > lastImportedDate;
              })
            )
            .pipe(new StreamFilterTransform<ERC20TransferFromEventData>((row) => row.value !== "0"))
            .pipe(new StreamBatchBy<ERC20TransferFromEventData>((row) => row.blockNumber))
            .pipe(
              transform((transferBatch: ERC20TransferFromEventData[]) => {
                if (strategyHarvestCount === null || strategyHarvestCount === 1) {
                  strategyHarvestCount = transferBatch.length;
                }
                return transferBatchToFeeReports(
                  chain,
                  vault,
                  strategy.implementation,
                  strategyHarvestCount,
                  roleAddressMap,
                  transferBatch
                );
              })
            )
            .pipe(new FlattenStream())
            .pipe(
              new StreamAggBy<FeeReportRow>(
                (row) => dateTruncToDay(row.datetime).getTime(),
                (rows) => {
                  return {
                    datetime: dateTruncToDay(rows[0].datetime),
                    harvest_count: sumBy(rows, (r) => r.harvest_count),
                    caller_wnative_amount: rows
                      .map((r) => r.caller_wnative_amount)
                      .reduce((agg, v) => agg.add(v), BIG_ZER0),
                    strategist_wnative_amount: rows
                      .map((r) => r.strategist_wnative_amount)
                      .reduce((agg, v) => agg.add(v), BIG_ZER0),
                    beefy_wnative_amount: rows
                      .map((r) => r.beefy_wnative_amount)
                      .reduce((agg, v) => agg.add(v), BIG_ZER0),
                    compound_wnative_amount: rows
                      .map((r) => r.compound_wnative_amount)
                      .reduce((agg, v) => agg.add(v), BIG_ZER0),
                    ukn_wnative_amount: rows.map((r) => r.ukn_wnative_amount).reduce((agg, v) => agg.add(v), BIG_ZER0),
                  };
                }
              )
            )
        );
      },
      getLastDbRowDate: async () => lastImportedDate,
      getLastFileDate: async () =>
        (await erc20TransferFromStore.getLastRow(chain, strategy.implementation, wnativeTokenAddress))?.datetime ||
        null,
      rowToDbTransformer: (reportRow: FeeReportRow) => {
        return [
          // these should match the order of the copy cmd
          chain,
          vault.id,
          reportRow.datetime.toISOString(),
          strAddressToPgBytea(strategy.implementation),
          reportRow.harvest_count,
          formatBigNumber(reportRow.caller_wnative_amount, wnativeTokenDecimals),
          formatBigNumber(reportRow.strategist_wnative_amount, wnativeTokenDecimals),
          formatBigNumber(reportRow.beefy_wnative_amount, wnativeTokenDecimals),
          formatBigNumber(reportRow.compound_wnative_amount, wnativeTokenDecimals),
          formatBigNumber(reportRow.ukn_wnative_amount, wnativeTokenDecimals),
        ];
      },
      flatten: false,
    });
  }
}

async function loadCSVStreamToTimescaleTable<CSVObjType extends { datetime: Date }>(opts: {
  logKey: string;
  getFileStreamAfterDate: (date: Date | null) => Promise<Transform | null>;
  getLastDbRowDate: () => Promise<Date | null>;
  getLastFileDate: () => Promise<Date | null>;
  dbCopyQuery: string;
  rowToDbTransformer: (row: CSVObjType) => any[];
  flatten: boolean;
}) {
  const pgPool = await getPgPool();

  logger.info(`[LTSDB] Importing ${opts.logKey}`);

  // now get the last imported data to filter on those
  const lastImportedDate = await opts.getLastDbRowDate();
  if (lastImportedDate) {
    const lastLocalTransfer = await opts.getLastFileDate();
    if (lastLocalTransfer && lastLocalTransfer > lastImportedDate) {
      logger.verbose(`[LTSDB] Only importing ${opts.logKey} events after ${lastImportedDate.toISOString()}`);
    } else {
      logger.verbose(`[LTSDB] Nothing to import for ${opts.logKey}`);
    }
  } else {
    logger.verbose(`[LTSDB] No data in database for ${opts.logKey}, importing all events`);
  }

  const fileReadStream = await opts.getFileStreamAfterDate(lastImportedDate);
  if (!fileReadStream) {
    logger.verbose(`[LTSDB] No data in input stream for ${opts.logKey}`);
    return;
  }

  await new Promise((resolve, reject) => {
    pgPool.connect(function (err, client, poolCallback) {
      function onOk(...args: any[]) {
        poolCallback(...args);
        resolve(args);
      }
      let errorHandled = false;
      function onErr(error: Error) {
        logger.error(`[LTSDB] Error on import stream ${opts.logKey}: `, error);
        if (errorHandled) {
          return;
        }
        errorHandled = true;
        poolCallback(error);
        reject(error);
      }
      if (err) {
        return reject(err);
      }
      const dbCopyStream = client.query(copyFrom(opts.dbCopyQuery));
      fileReadStream.on("error", onErr);
      dbCopyStream.on("error", onErr);
      dbCopyStream.on("finish", onOk);
      // start the work
      let stream: Transform = fileReadStream
        // transform js obj to something the db understands
        .pipe(transform(opts.rowToDbTransformer));

      stream.on("error", onErr);

      if (opts.flatten) {
        stream = stream.pipe(new FlattenStream());
      }

      // transform to csv
      stream = stream.pipe(stringify({ header: false }));

      // send this to the database
      stream.pipe(dbCopyStream);
    });
  });

  logger.debug(`[LTSDB] Finished processing ${opts.logKey}`);
}

runMain(main);

export class InconsistentUserBalance extends Error {
  constructor(
    chain: Chain,
    contractAddress: string,
    lastBalance: ethers.BigNumber,
    newBalance: ethers.BigNumber,
    data: ERC20EventData
  ) {
    super(
      `Refusing to insert negative balance for non-mintburn address ${chain}:${contractAddress}: ${JSON.stringify({
        data,
        lastBalance: lastBalance.toString(),
        newBalance: newBalance.toString(),
      })}`
    );
  }
}
