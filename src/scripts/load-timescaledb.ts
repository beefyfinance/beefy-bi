import { from as copyFrom } from "pg-copy-streams";
import { runMain } from "../utils/process";
import yargs from "yargs";
import { allChainIds } from "../types/chain";
import { transform } from "stream-transform";
import { Chain } from "../types/chain";
import { stringify } from "csv-stringify";
import { db_query_one, getPgPool, strAddressToPgBytea } from "../utils/db";
import { getLocalBeefyVaultList } from "../lib/fetch-if-not-found-locally";
import { logger } from "../utils/logger";
import {
  ERC20EventData,
  getErc20TransferEventsStream,
} from "../lib/csv-transfer-events";
import { StreamObjectFilterTransform } from "../utils/stream";

async function main() {
  const argv = await yargs(process.argv.slice(2))
    .usage("Usage: $0 [options]")
    .options({
      chain: { choices: [...allChainIds, "all"], alias: "c", demand: true },
      vaultId: { type: "string", demand: false, alias: "v" },
    }).argv;

  const chain = argv.chain as Chain | "all";
  const chains = chain === "all" ? allChainIds : [chain];
  const vaultId = argv.vaultId || null;

  const pgPool = await getPgPool();

  for (const chain of chains) {
    const vaults = await getLocalBeefyVaultList(chain);
    for (const vault of vaults) {
      if (vaultId && vault.id !== vaultId) {
        logger.verbose(`[LTSDB] Skipping ${chain}:${vault.id}`);
        continue;
      }
      logger.verbose(`[LTSDB] Processing ${chain}:${vault.id}`);
      const contractAddress = vault.token_address;

      const fileReadStream = await getErc20TransferEventsStream(
        chain,
        contractAddress
      );
      if (!fileReadStream) {
        logger.verbose(`[LTSDB] No data for ${chain}:${vault.id}`);
        continue;
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
        logger.verbose(
          `[LTSDB] Only importing ${chain}:${
            vault.id
          } events after ${lastImportedDate.toISOString()}`
        );
      } else {
        logger.verbose(
          `[LTSDB] No data for ${chain}:${vault.id}, importing all events`
        );
      }

      const onlyLatestRowsFilter =
        new StreamObjectFilterTransform<ERC20EventData>((row) => {
          if (!lastImportedDate) {
            return true;
          }
          return row.datetime > lastImportedDate;
        });

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
  }
}

runMain(main);
