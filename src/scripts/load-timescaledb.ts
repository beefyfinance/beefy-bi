import { from as copyFrom } from "pg-copy-streams";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { allChainIds } from "../types/chain";
import { transform } from "stream-transform";
import { Chain } from "../types/chain";
import { stringify } from "csv-stringify";
import { db_query_one, getPgPool, strAddressToPgBytea } from "../utils/db";
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

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: false, alias: "v" },
      importOnly: {
        choices: ["erc20_transfers", "ppfs", "prices"],
        alias: "o",
        demand: false,
      },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId || null;
  const importOnly = argv.importOnly || null;

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

  logger.info("[LTSDB] Finished importing data. Sleeping 4h...");
  await sleep(4 * 60 * 60 * 1000);
}

async function importVaultERC20TransfersToDB(chain: Chain, vault: BeefyVault) {
  const pgPool = await getPgPool();

  logger.info(`[LTSDB] Importing ${chain}:${vault.id}`);
  const contractAddress = vault.token_address;

  const fileReadStream = await getErc20TransferEventsStream(
    chain,
    contractAddress
  );
  if (!fileReadStream) {
    logger.verbose(`[LTSDB] No data for ${chain}:${vault.id}`);
    return;
  }
  // now get the last imported data to filter on those
  const lastImportedDate =
    (
      await db_query_one<{ last_imported: Date }>(
        `SELECT max(datetime) as last_imported
        FROM beefy_raw.erc20_transfer_ts
        WHERE chain = %L
          AND contract_address = %L`,
        [chain, strAddressToPgBytea(contractAddress)]
      )
    )?.last_imported || null;

  if (lastImportedDate) {
    const lastLocalTransfer = await getLastImportedERC20TransferEvent(
      chain,
      contractAddress
    );
    if (lastLocalTransfer && lastLocalTransfer.datetime > lastImportedDate) {
      logger.verbose(
        `[LTSDB] Only importing ${chain}:${
          vault.id
        } events after ${lastImportedDate.toISOString()}`
      );
    } else {
      logger.verbose(`[LTSDB] Nothing to import for ${chain}:${vault.id} `);
    }
  } else {
    logger.verbose(
      `[LTSDB] No data for ${chain}:${vault.id}, importing all events`
    );
  }

  const onlyLatestRowsFilter = new StreamObjectFilterTransform<ERC20EventData>(
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
      const stream = client.query(
        copyFrom(
          `COPY beefy_raw.erc20_transfer_ts (chain, contract_address, datetime, from_address, to_address, value) 
              FROM STDIN
              WITH CSV DELIMITER ',';`
        )
      );
      fileReadStream.on("error", onErr);
      stream.on("error", onErr);
      stream.on("finish", onOk);
      // start the work
      fileReadStream
        .pipe(onlyLatestRowsFilter)
        .pipe(
          transform(function (data: ERC20EventData) {
            return [
              // these should match the order of the copy cmd
              chain,
              strAddressToPgBytea(contractAddress),
              data.datetime.toISOString(),
              strAddressToPgBytea(data.from),
              strAddressToPgBytea(data.to),
              data.value,
            ];
          })
        )
        .pipe(
          stringify({
            header: false,
          })
        )
        .pipe(stream);
    });
  });

  logger.debug(`[LTSDB] Finished processing ${chain}:${vault.id}`);
}

async function importVaultPPFSToDB(chain: Chain, vault: BeefyVault) {
  const samplingPeriod = "4hour";
  const pgPool = await getPgPool();

  logger.info(`[LTSDB] Importing ${chain}:${vault.id}`);
  const contractAddress = vault.token_address;

  const fileReadStream = getBeefyVaultV6PPFSDataStream(
    chain,
    contractAddress,
    samplingPeriod
  );
  if (!fileReadStream) {
    logger.verbose(`[LTSDB] No data for ${chain}:${vault.id}`);
    return;
  }
  // now get the last imported data to filter on those
  const lastImportedDate =
    (
      await db_query_one<{ last_imported: Date }>(
        `SELECT max(datetime) as last_imported
        FROM beefy_raw.vault_ppfs_ts
        WHERE chain = %L
          AND contract_address = %L`,
        [chain, strAddressToPgBytea(contractAddress)]
      )
    )?.last_imported || null;

  if (lastImportedDate) {
    const lastLocalPpfs = await getLastImportedBeefyVaultV6PPFSData(
      chain,
      contractAddress,
      samplingPeriod
    );
    if (lastLocalPpfs && lastLocalPpfs.datetime > lastImportedDate) {
      logger.verbose(
        `[LTSDB] Only importing ${chain}:${
          vault.id
        } events after ${lastImportedDate.toISOString()}`
      );
    } else {
      logger.verbose(`[LTSDB] Nothing to import for ${chain}:${vault.id} `);
    }
  } else {
    logger.verbose(
      `[LTSDB] No data for ${chain}:${vault.id}, importing all events`
    );
  }

  const onlyLatestRowsFilter = new StreamObjectFilterTransform<ERC20EventData>(
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
      const stream = client.query(
        copyFrom(
          `COPY beefy_raw.vault_ppfs_ts (chain, contract_address, datetime, ppfs) 
              FROM STDIN
              WITH CSV DELIMITER ',';`
        )
      );
      fileReadStream.on("error", onErr);
      stream.on("error", onErr);
      stream.on("finish", onOk);
      // start the work
      fileReadStream
        .pipe(onlyLatestRowsFilter)
        .pipe(
          transform(function (data: BeefyVaultV6PPFSData) {
            return [
              // these should match the order of the copy cmd
              chain,
              strAddressToPgBytea(contractAddress),
              data.datetime.toISOString(),
              data.pricePerFullShare,
            ];
          })
        )
        .pipe(
          stringify({
            header: false,
          })
        )
        .pipe(stream);
    });
  });

  logger.debug(`[LTSDB] Finished processing ${chain}:${vault.id}`);
}

runMain(main);
