import { from as copyFrom } from "pg-copy-streams";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { allChainIds } from "../types/chain";
import { transform } from "stream-transform";
import { Chain } from "../types/chain";
import { stringify } from "csv-stringify";
import {
  db_query,
  db_query_one,
  getPgPool,
  strAddressToPgBytea,
  strArrToPgStrArr,
} from "../utils/db";
import {
  BeefyVault,
  getLocalBeefyVaultList,
} from "../lib/fetch-if-not-found-locally";
import { logger } from "../utils/logger";
import {
  ERC20EventData,
  getErc20TransferEventsStream,
  getLastImportedERC20TransferEvent,
} from "../lib/csv-transfer-events";
import { StreamObjectFilterTransform } from "../utils/stream";
import { sleep } from "../utils/async";
import {
  BeefyVaultV6PPFSData,
  getBeefyVaultV6PPFSDataStream,
  getLastImportedBeefyVaultV6PPFSData,
} from "../lib/csv-vault-ppfs";
import { Transform } from "stream";
import {
  getAllAvailableOracleIds,
  getLastImportedOraclePrice,
  getOraclePricesStream,
  OraclePriceData,
} from "../lib/csv-oracle-price";
import { SamplingPeriod } from "../lib/csv-block-samples";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: false, alias: "v" },
      importOnly: {
        choices: ["erc20_transfers", "ppfs", "prices", "vaults"],
        alias: "o",
        demand: false,
      },
    }).argv;
  type ImportOnly = "erc20_transfers" | "ppfs" | "prices" | "vaults";
  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId || null;
  const importOnly: ImportOnly | null = (argv.importOnly as ImportOnly) || null;

  if (!importOnly || importOnly === "erc20_transfers") {
    logger.info(`[LTSDB] Importing ERC20 transfers`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing ERC20 transfers for ${chain}`);
      const vaults = await getLocalBeefyVaultList(chain);
      for (const vault of vaults) {
        if (vaultId && vault.id !== vaultId) {
          logger.verbose(
            `[LTSDB] Skipping ERC20 transfers for ${chain}:${vault.id}`
          );
          continue;
        }
        try {
          await importVaultERC20TransfersToDB(chain, vault);
        } catch (err) {
          logger.error(
            `[LTSDB] Skipping ERC20 transfers for ${chain}:${
              vault.id
            }. ${JSON.stringify(err)}`
          );
        }
      }
    }
  }

  if (!importOnly || importOnly === "ppfs") {
    logger.info(`[LTSDB] Importing ppfs`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing ppfs for ${chain}`);
      const vaults = await getLocalBeefyVaultList(chain);
      for (const vault of vaults) {
        if (vaultId && vault.id !== vaultId) {
          logger.verbose(`[LTSDB] Skipping ppfs for ${chain}:${vault.id}`);
          continue;
        }
        try {
          await importVaultPPFSToDB(chain, vault);
        } catch (err) {
          logger.error(
            `[LTSDB] Skipping ppfs for vault ${chain}:${
              vault.id
            }. ${JSON.stringify(err)}`
          );
        }
      }
    }
  }

  if (!importOnly || importOnly === "prices") {
    logger.info(`[LTSDB] Importing prices`);

    const oracleIds = getAllAvailableOracleIds("15min");
    for await (const oracleId of oracleIds) {
      await importPricesToDB(oracleId);
    }
  }

  if (!importOnly || importOnly === "vaults") {
    logger.info(`[LTSDB] Importing vaults`);
    for (const chain of chains) {
      logger.info(`[LTSDB] Importing vaults for ${chain}`);
      const vaults = await getLocalBeefyVaultList(chain);
      if (vaults.length <= 0) {
        logger.verbose(`[LTSDB] No vaults found for ${chain}`);
        continue;
      }
      await db_query(
        `
        INSERT INTO beefy_raw.vault (
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
          vault_id = beefy_raw.vault.vault_id,
          token_name = beefy_raw.vault.token_name,
          want_address = beefy_raw.vault.want_address,
          want_decimals = beefy_raw.vault.want_decimals,
          want_price_oracle_id = beefy_raw.vault.want_price_oracle_id,
          end_of_life = beefy_raw.vault.end_of_life,
          assets_oracle_id = beefy_raw.vault.assets_oracle_id
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

  logger.info("[LTSDB] Finished importing data. Sleeping 4h...");
  await sleep(4 * 60 * 60 * 1000);
}

async function importVaultERC20TransfersToDB(chain: Chain, vault: BeefyVault) {
  const contractAddress = vault.token_address;
  return loadCSVStreamToTimescaleTable({
    logKey: `ERC20 Transfers for ${chain}:${vault.id}`,

    dbCopyQuery: `COPY beefy_raw.erc20_transfer_ts (chain, contract_address, datetime, from_address, to_address, value) 
        FROM STDIN
        WITH CSV DELIMITER ',';`,

    getFileStream: async () =>
      getErc20TransferEventsStream(chain, contractAddress),

    getLastDbRowDate: async () =>
      (
        await db_query_one<{ last_imported: Date }>(
          `SELECT max(datetime) as last_imported
          FROM beefy_raw.erc20_transfer_ts
          WHERE chain = %L
            AND contract_address = %L`,
          [chain, strAddressToPgBytea(contractAddress)]
        )
      )?.last_imported || null,

    getLastFileDate: async () =>
      (await getLastImportedERC20TransferEvent(chain, contractAddress))
        ?.datetime || null,

    rowToDbTransformer: (data: ERC20EventData) => {
      return [
        // these should match the order of the copy cmd
        chain,
        strAddressToPgBytea(contractAddress),
        data.datetime.toISOString(),
        strAddressToPgBytea(data.from),
        strAddressToPgBytea(data.to),
        data.value,
      ];
    },
  });
}

async function importVaultPPFSToDB(chain: Chain, vault: BeefyVault) {
  const samplingPeriod = "4hour";
  const contractAddress = vault.token_address;

  return loadCSVStreamToTimescaleTable({
    logKey: `PPFS for ${chain}:${vault.id}`,

    dbCopyQuery: `COPY beefy_raw.vault_ppfs_ts (chain, contract_address, datetime, ppfs) FROM STDIN WITH CSV DELIMITER ',';`,

    getFileStream: async () =>
      getBeefyVaultV6PPFSDataStream(chain, contractAddress, samplingPeriod),

    getLastDbRowDate: async () =>
      (
        await db_query_one<{ last_imported: Date }>(
          `SELECT max(datetime) as last_imported
          FROM beefy_raw.vault_ppfs_ts
          WHERE chain = %L
            AND contract_address = %L`,
          [chain, strAddressToPgBytea(contractAddress)]
        )
      )?.last_imported || null,

    getLastFileDate: async () =>
      (
        await getLastImportedBeefyVaultV6PPFSData(
          chain,
          contractAddress,
          samplingPeriod
        )
      )?.datetime || null,

    rowToDbTransformer: (data: BeefyVaultV6PPFSData) => {
      return [
        // these should match the order of the copy cmd
        chain,
        strAddressToPgBytea(contractAddress),
        data.datetime.toISOString(),
        data.pricePerFullShare,
      ];
    },
  });
}

async function importPricesToDB(oracleId: string) {
  const samplingPeriod: SamplingPeriod = "15min";

  return loadCSVStreamToTimescaleTable({
    logKey: `prices for ${oracleId}`,

    dbCopyQuery: `COPY beefy_raw.oracle_price_ts(oracle_id, datetime, usd_value) FROM STDIN WITH CSV DELIMITER ',';`,

    getFileStream: async () => getOraclePricesStream(oracleId, samplingPeriod),

    getLastDbRowDate: async () =>
      (
        await db_query_one<{ last_imported: Date }>(
          `SELECT max(datetime) as last_imported
          FROM beefy_raw.oracle_price_ts
          WHERE oracle_id = %L`,
          [oracleId]
        )
      )?.last_imported || null,

    getLastFileDate: async () =>
      (await getLastImportedOraclePrice(oracleId, samplingPeriod))?.datetime ||
      null,

    rowToDbTransformer: (data: OraclePriceData) => {
      return [
        // these should match the order of the copy cmd
        oracleId,
        data.datetime.toISOString(),
        data.usdValue,
      ];
    },
  });
}

async function loadCSVStreamToTimescaleTable<
  CSVObjType extends { datetime: Date }
>(opts: {
  logKey: string;
  getFileStream: () => Promise<Transform | null>;
  getLastDbRowDate: () => Promise<Date | null>;
  getLastFileDate: () => Promise<Date | null>;
  dbCopyQuery: string;
  rowToDbTransformer: (row: CSVObjType) => any[];
}) {
  const pgPool = await getPgPool();

  logger.info(`[LTSDB] Importing ${opts.logKey}`);

  const fileReadStream = await opts.getFileStream();
  if (!fileReadStream) {
    logger.verbose(`[LTSDB] No data for ${opts.logKey}`);
    return;
  }
  // now get the last imported data to filter on those
  const lastImportedDate = await opts.getLastDbRowDate();
  if (lastImportedDate) {
    const lastLocalTransfer = await opts.getLastFileDate();
    if (lastLocalTransfer && lastLocalTransfer > lastImportedDate) {
      logger.verbose(
        `[LTSDB] Only importing ${
          opts.logKey
        } events after ${lastImportedDate.toISOString()}`
      );
    } else {
      logger.verbose(`[LTSDB] Nothing to import for ${opts.logKey}`);
    }
  } else {
    logger.verbose(`[LTSDB] No data for ${opts.logKey}, importing all events`);
  }

  const onlyLatestRowsFilter = new StreamObjectFilterTransform<CSVObjType>(
    (row) => {
      if (!lastImportedDate) {
        return true;
      }
      return row.datetime > lastImportedDate;
    }
  );

  await new Promise((resolve, reject) => {
    pgPool.connect(function (err, client, poolCallback) {
      function onOk(...args: any[]) {
        poolCallback(...args);
        resolve(args);
      }
      function onErr(error: Error) {
        poolCallback(error);
        reject(error);
      }
      if (err) {
        return reject(err);
      }
      const stream = client.query(copyFrom(opts.dbCopyQuery));
      fileReadStream.on("error", onErr);
      stream.on("error", onErr);
      stream.on("finish", onOk);
      // start the work
      fileReadStream
        // only relevant rows
        .pipe(onlyLatestRowsFilter)
        // transform js obj to something the db understands
        .pipe(transform(opts.rowToDbTransformer))
        // transform to csv
        .pipe(stringify({ header: false }))
        // send this to the database
        .pipe(stream);
    });
  });

  logger.debug(`[LTSDB] Finished processing ${opts.logKey}`);
}

runMain(main);
