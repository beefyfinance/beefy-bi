import { DB_URL } from "./config";
import * as pgcs from "pg-connection-string";
import { Pool, PoolConfig } from "pg";
import pgf from "pg-format";
import { logger } from "./logger";
import { Chain } from "../types/chain";

const config = pgcs.parse(DB_URL);
const pool = new Pool(config as any as PoolConfig);
migrate();

export async function db_query<RowType>(
  sql: string,
  params: any[] = []
): Promise<RowType[]> {
  logger.debug(`Executing query: ${sql}, params: ${params}`);
  const res = await pool.query(pgf(sql, ...params));
  const rows = res?.rows || null;
  logger.debug(`Got ${res?.rowCount} for query: ${sql}, params ${params}`);
  return rows;
}

export async function db_query_one<RowType>(
  sql: string,
  params: any[] = []
): Promise<RowType | null> {
  const rows = await db_query<RowType>(sql, params);
  if (rows.length === 0) {
    return null;
  }
  return rows[0];
}

async function migrate() {
  return db_query(`
      CREATE TABLE IF NOT EXISTS erc20_transfer (
          chain text NOT NULL,
          contract_address text NOT NULL,
          time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
          block_number INTEGER NOT NULL,
          from_address text not null,
          to_address text not null,
          value text not null
      ) PARTITION BY LIST (contract_address);

      CREATE SCHEMA IF NOT EXISTS partitions;
  `);
}

export async function prepareInsertErc20TransferBatch(
  _: Chain,
  contractAddress: string
) {
  return db_query(
    `
    CREATE TABLE IF NOT EXISTS partitions."erc20_transfer_%s" PARTITION OF erc20_transfer
    FOR VALUES IN (%L);
  `,
    [contractAddress, contractAddress]
  );
}

export async function insertErc20TransferBatch(
  values: {
    chain: string;
    contract_address: string;
    time: string;
    block_number: number;
    from_address: string;
    to_address: string;
    value: string;
  }[]
) {
  logger.verbose(`[DB] Inserting ${values.length} rows into erc20_transfer`);
  const valueArr = values.map((val) => [
    val.chain,
    val.contract_address,
    val.time,
    val.block_number,
    val.from_address,
    val.to_address,
    val.value,
  ]);
  return db_query(
    `
    INSERT INTO erc20_transfer (
      chain,
      contract_address,
      time,
      block_number,
      from_address,
      to_address,
      value
    ) VALUES %L
    ON CONFLICT DO NOTHING
  `,
    [valueArr]
  );
}
