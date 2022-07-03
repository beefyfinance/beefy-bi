import { Pool, PoolConfig } from "pg";
import pgf from "pg-format";
import { logger } from "./logger";
import * as pgcs from "pg-connection-string";
import { TIMESCALEDB_URL } from "./config";
import { normalizeAddress } from "./ethers";

/**
 * evm_address: It's a bit more difficult to use but half the size
 * 
beefy=# select 
    octet_length('\x2BdfBd329984Cf0DC9027734681A16f542cF3bB4'::bytea) as bytea_addr_size, 
    octet_length('0x2BdfBd329984Cf0DC9027734681A16f542cF3bB4') as str_addr_size;
-[ RECORD 1 ]---+---
bytea_addr_size | 20
str_addr_size   | 42
 */

let pool: Pool | null = null;
export async function getPgPool() {
  if (pool === null) {
    const config = pgcs.parse(TIMESCALEDB_URL) as PoolConfig;
    pool = new Pool(config);
    await migrate();
  }
  return pool;
}

export async function db_query<RowType>(
  sql: string,
  params: any[] = []
): Promise<RowType[]> {
  logger.debug(`Executing query: ${sql}, params: ${params}`);
  const pool = await getPgPool();
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

export function strAddressToPgBytea(evmAddress: string) {
  // 0xABC -> // \xABC
  return "\\x" + normalizeAddress(evmAddress).slice(2);
}

async function migrate() {
  // postgresql don't have "create type/domain if not exists"
  async function typeExists(typeName: string) {
    const res = await db_query_one(`SELECT * FROM pg_type WHERE typname = %L`, [
      typeName,
    ]);
    return res !== null;
  }

  // avoid error ERROR:  cannot change configuration on already compressed chunks
  // on alter table set compression
  async function isCompressionEnabled(
    hyperTableSchema: string,
    hypertableName: string
  ) {
    const res = await db_query_one<{ compression_enabled: boolean }>(
      `SELECT compression_enabled 
      FROM timescaledb_information.hypertables 
      WHERE hypertable_schema = %L
      AND hypertable_name = %L`,
      [hyperTableSchema, hypertableName]
    );
    if (res === null) {
      throw new Error(`No hypertable ${hyperTableSchema}.${hypertableName}`);
    }
    return res.compression_enabled;
  }

  // types
  if (!(await typeExists("chain_enum"))) {
    await db_query(`
        CREATE TYPE chain_enum AS ENUM ('ethereum');
    `);
  }
  await db_query(`
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'arbitrum';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'aurora';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'avax';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'bsc';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'celo';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'cronos';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'emerald';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'fantom';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'fuse';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'harmony';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'heco';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'metis';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'moonbeam';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'moonriver';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'optimism';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'polygon';
    ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS 'syscoin';
  `);

  if (!(await typeExists("evm_address"))) {
    await db_query(`
      CREATE DOMAIN evm_address AS BYTEA NOT NULL;
    `);
  }

  if (!(await typeExists("uint_256"))) {
    await db_query(`
      CREATE DOMAIN uint_256 
        AS NUMERIC NOT NULL
        CHECK (VALUE >= 0 AND VALUE < 2^256)
        CHECK (SCALE(VALUE) = 0)
    `);
  }

  // schemas
  await db_query(`
    CREATE SCHEMA IF NOT EXISTS beefy_raw;
    CREATE SCHEMA IF NOT EXISTS beefy_derived;
    CREATE SCHEMA IF NOT EXISTS beefy_report;
  `);

  // helper function
  await db_query(`
    CREATE OR REPLACE FUNCTION format_evm_address(bytea) RETURNS character varying 
      AS $$
        SELECT '0x' || encode($1::bytea, 'hex')
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;
  `);

  // transfer table
  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_raw.erc20_transfer_ts (
      chain chain_enum NOT NULL,
      contract_address evm_address NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      from_address evm_address not null,
      to_address evm_address not null,
      value uint_256 not null
    );
    SELECT create_hypertable(
      relation => 'beefy_raw.erc20_transfer_ts', 
      time_column_name => 'datetime', 
      -- partitioning_column => 'chain',
      -- number_partitions => 20,
      chunk_time_interval => INTERVAL '7 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("beefy_raw", "erc20_transfer_ts"))) {
    await db_query(`
      ALTER TABLE beefy_raw.erc20_transfer_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'chain, contract_address',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'beefy_raw.erc20_transfer_ts', 
        compress_after => INTERVAL '10 days', -- keep a margin as data will arrive in batches
        if_not_exists => true
      );
    `);
  }

  // PPFS
  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_raw.vault_ppfs_ts (
      chain chain_enum NOT NULL,
      contract_address evm_address NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      ppfs uint_256 not null
    );
    SELECT create_hypertable(
      relation => 'beefy_raw.vault_ppfs_ts', 
      time_column_name => 'datetime', 
      -- partitioning_column => 'chain',
      -- number_partitions => 20,
      chunk_time_interval => INTERVAL '14 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("beefy_raw", "vault_ppfs_ts"))) {
    await db_query(`
      ALTER TABLE beefy_raw.vault_ppfs_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'chain, contract_address',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'beefy_raw.vault_ppfs_ts', 
        compress_after => INTERVAL '20 days', -- keep a margin as data will arrive in batches
        if_not_exists => true
      );
    `);
  }

  // PPFS
  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_raw.oracle_price_ts (
      oracle_id varchar NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      usd_value double precision not null
    );
    SELECT create_hypertable(
      relation => 'beefy_raw.oracle_price_ts', 
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '14 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("beefy_raw", "oracle_price_ts"))) {
    await db_query(`
      ALTER TABLE beefy_raw.oracle_price_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'oracle_id',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'beefy_raw.oracle_price_ts', 
        compress_after => INTERVAL '20 days', -- keep a margin as data will arrive in batches
        if_not_exists => true
      );
    `);
  }
}
