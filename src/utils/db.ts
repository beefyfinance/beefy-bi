import { Pool, PoolConfig } from "pg";
import pgf from "pg-format";
import { logger } from "./logger";
import * as pgcs from "pg-connection-string";
import { TIMESCALEDB_URL } from "./config";
import { normalizeAddress } from "./ethers";
import { allChainIds } from "../types/chain";

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
  const sql_w_params = pgf(sql, ...params);
  //console.log(sql_w_params);
  const res = await pool.query(sql_w_params);
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

export function strArrToPgStrArr(strings: string[]) {
  return "{" + pgf.withArray("%L", strings) + "}";
}

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

async function migrate() {
  // types
  if (!(await typeExists("chain_enum"))) {
    await db_query(`
        CREATE TYPE chain_enum AS ENUM ('ethereum');
    `);
  }
  for (const chain of allChainIds) {
    await db_query(`ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS %L`, [chain]);
  }

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

  if (!(await typeExists("int_256"))) {
    await db_query(`
      CREATE DOMAIN int_256 
        AS NUMERIC NOT NULL
        CHECK (VALUE >= -2^255 AND VALUE < (2^255)-1)
        CHECK (SCALE(VALUE) = 0)
    `);
  }

  // schemas
  await db_query(`
    CREATE SCHEMA IF NOT EXISTS data_raw;
    CREATE SCHEMA IF NOT EXISTS data_derived;
    CREATE SCHEMA IF NOT EXISTS data_report;
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

    CREATE OR REPLACE FUNCTION evm_address_to_bytea(varchar) RETURNS bytea 
      AS $$
        select decode(substring($1 ,3), 'hex')
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;
  `);

  // balance diff table
  await db_query(`
    CREATE TABLE IF NOT EXISTS data_raw.erc20_balance_diff_ts (
      chain chain_enum NOT NULL,
      contract_address evm_address NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      owner_address evm_address not null,
      balance_diff int_256 not null,
      balance_before int_256 not null,
      balance_after int_256 not null
    );
    SELECT create_hypertable(
      relation => 'data_raw.erc20_balance_diff_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("data_raw", "erc20_balance_diff_ts"))) {
    await db_query(`
      ALTER TABLE data_raw.erc20_balance_diff_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'chain, contract_address',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'data_raw.erc20_balance_diff_ts', 
        compress_after => INTERVAL '10 days', -- keep a margin as data will arrive in batches
        if_not_exists => true
      );
    `);
  }

  // PPFS
  await db_query(`
    CREATE TABLE IF NOT EXISTS data_raw.vault_ppfs_ts (
      chain chain_enum NOT NULL,
      contract_address evm_address NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      ppfs uint_256 not null
    );
    SELECT create_hypertable(
      relation => 'data_raw.vault_ppfs_ts', 
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '14 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("data_raw", "vault_ppfs_ts"))) {
    await db_query(`
      ALTER TABLE data_raw.vault_ppfs_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'chain, contract_address',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'data_raw.vault_ppfs_ts', 
        compress_after => INTERVAL '20 days', -- keep a margin as data will arrive in batches
        if_not_exists => true
      );
    `);
  }

  // oracle price
  await db_query(`
    CREATE TABLE IF NOT EXISTS data_raw.oracle_price_ts (
      oracle_id varchar NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      usd_value double precision not null
    );
    SELECT create_hypertable(
      relation => 'data_raw.oracle_price_ts', 
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '14 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("data_raw", "oracle_price_ts"))) {
    await db_query(`
      ALTER TABLE data_raw.oracle_price_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'oracle_id',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'data_raw.oracle_price_ts', 
        compress_after => INTERVAL '20 days', -- keep a margin as data will arrive in batches
        if_not_exists => true
      );
    `);
  }

  await db_query(`
    CREATE TABLE IF NOT EXISTS data_raw.vault (
      chain chain_enum NOT NULL,
      token_address evm_address NOT NULL,
      vault_id varchar NOT NULL,
      token_name varchar NOT NULL,
      want_address evm_address NOT NULL,
      want_decimals INTEGER NOT NULL,
      want_price_oracle_id varchar NOT NULL,
      end_of_life boolean not null,
      assets_oracle_id varchar[] not null,
      PRIMARY KEY(chain, token_address)
    );
  `);

  // continuous aggregates: ppfs
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS data_derived.vault_ppfs_4h_ts WITH (timescaledb.continuous)
      AS select ts.chain, ts.contract_address, 
          time_bucket('4h', ts.datetime) as datetime, 
          avg(ts.ppfs) as avg_ppfs
      from data_raw.vault_ppfs_ts ts
      group by 1,2,3;
  `);
  // continuous aggregates: prices
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS data_derived.oracle_price_4h_ts WITH (timescaledb.continuous)
      AS select oracle_id,
          time_bucket('4h', datetime) as datetime, 
          avg(usd_value) as avg_usd_value
      from data_raw.oracle_price_ts
      group by 1,2;
  `);

  // continuous aggregates: transfer diffs on the contract/owner level
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS data_derived.erc20_owner_balance_diff_4h_ts WITH (timescaledb.continuous)
      AS 
      select chain, contract_address, owner_address,
        time_bucket('4h', datetime) as datetime, 
        sum(balance_diff)  as balance_diff,
        last(balance_after, datetime) - sum(balance_diff) as balance_before,
        last(balance_after, datetime) as balance_after,
        coalesce(sum(balance_diff) filter(where balance_diff > 0),0) as deposit_diff,
        coalesce(sum(balance_diff) filter(where balance_diff < 0),0) as withdraw_diff,
        count(*) as trx_count,
        count(*) filter (where balance_diff > 0) as deposit_count,
        count(*) filter (where balance_diff < 0) as withdraw_count
      from data_raw.erc20_balance_diff_ts
      group by 1,2,3,4;
  `);

  // continuous aggregates: transfer diffs on the contract level
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS data_derived.erc20_contract_balance_diff_4h_ts WITH (timescaledb.continuous)
      AS
        select chain, contract_address,
          time_bucket('4h', datetime) as datetime, 
          last(-balance_after, datetime) - first(-balance_before, datetime) as balance_diff,
          first(-balance_before, datetime) as balance_before,
          last(-balance_after, datetime) as balance_after,
          sum(-balance_diff) filter(where -balance_diff > 0) as deposit_diff,
          sum(-balance_diff) filter(where -balance_diff < 0) as withdraw_diff,
          count(*) as trx_count,
          count(*) filter (where -balance_diff > 0) as deposit_count,
          count(*) filter (where -balance_diff < 0) as withdraw_count
      from data_raw.erc20_balance_diff_ts
      -- only consider the minted tokens because it makes it easier to group
      -- just inverse the numbers to get the positive value
      where owner_address = evm_address_to_bytea('0x0000000000000000000000000000000000000000')
      group by 1,2,3;
  `);

  // helper materialized view to have a quick access to vault prices without indexing all prices
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS data_derived.vault_ppfs_and_price_4h_ts as 
      with vault_scope as (
          select chain, token_address, vault_id, want_price_oracle_id, want_decimals
          from data_raw.vault
      ), 
      want_prices_ts as (
          select oracle_id, datetime, avg_usd_value
          from data_derived.oracle_price_4h_ts
          where oracle_id in (
              select distinct scope.want_price_oracle_id 
              from vault_scope scope
          )
      ),
      ppfs_ts as (
          select chain, contract_address, datetime, avg_ppfs
          from data_derived.vault_ppfs_4h_ts ts
          where (chain, contract_address) in (
              select distinct scope.chain, scope.token_address
              from vault_scope scope
          )
      )
      select 
          v.chain, v.vault_id, v.token_address as contract_address, v.want_decimals, 
          coalesce(p.datetime, usd.datetime) as datetime, 
          p.avg_ppfs, usd.avg_usd_value as avg_want_usd_value
      from ppfs_ts p
      full outer join vault_scope v on v.chain = p.chain and v.token_address = p.contract_address
      full outer join want_prices_ts usd on usd.oracle_id = v.want_price_oracle_id and usd.datetime = p.datetime
      order by v.chain, v.vault_id, datetime
    ;
    CREATE INDEX IF NOT EXISTS idx_chain_vpp4h ON data_derived.vault_ppfs_and_price_4h_ts (chain);
    CREATE INDEX IF NOT EXISTS idx_address_vpp4h ON data_derived.vault_ppfs_and_price_4h_ts (contract_address);
    CREATE INDEX IF NOT EXISTS idx_datetime_vpp4h ON data_derived.vault_ppfs_and_price_4h_ts (datetime);
    -- index to speed up investor dashboard (5s -> 100ms)
    CREATE INDEX IF NOT EXISTS idx_chain_vault_dt_vpp4h ON data_derived.vault_ppfs_and_price_4h_ts (chain, vault_id, datetime);
  `);

  // pre-compute stats by vautl:
  // - total balance at each point in time
  // - owner_address hll to get distinct counts
  // - TVL at each point in time
  // - investment size buckets
  await db_query(`
    CREATE TABLE IF NOT EXISTS data_report.vault_stats_4h_ts (
      chain chain_enum NOT NULL,
      vault_id varchar NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      log_usd_owner_balance_histogram_1_1m_10b integer[] not null,
      owner_address_hll hyperloglog not null,
      usd_balance double precision -- prices can be null
    );
    SELECT create_hypertable(
      relation => 'data_report.vault_stats_4h_ts', 
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '14 days', 
      if_not_exists => true
    );

    CREATE INDEX IF NOT EXISTS idx_chain_vst4h ON data_report.vault_stats_4h_ts (chain);
    CREATE INDEX IF NOT EXISTS idx_vault_vst4h ON data_report.vault_stats_4h_ts (vault_id);
  `);
}

// this is kind of a continuous aggregate
// but we need a gapfill which is way faster to execute on a vault by vault basis
export async function rebuildVaultStatsReportTable() {
  // we select the min and max date to feed the gapfill function
  const contracts = await db_query<{
    chain: string;
    contract_address: string;
    vault_id: string;
    first_datetime: Date;
    last_datetime: Date;
  }>(`
    with contract_diff_dates as (
      SELECT
        chain,
        contract_address,
        min(datetime) as first_datetime,
        max(datetime) as last_datetime
      FROM data_derived.erc20_owner_balance_diff_4h_ts
      GROUP BY 1,2
    )
    select dates.chain, format_evm_address(dates.contract_address) as contract_address,
      dates.first_datetime, dates.last_datetime, vault.vault_id
    from contract_diff_dates dates
    join data_raw.vault vault on (dates.chain = vault.chain and dates.contract_address = vault.token_address)
    order by dates.chain, vault.vault_id
  `);

  for (const [idx, contract] of Object.entries(contracts)) {
    logger.info(
      `[DB] Refreshing vault stats for vault ${contract.chain}:${contract.vault_id} (${idx}/${contracts.length})`
    );
    await db_query(
      `
      BEGIN;
      DELETE FROM data_report.vault_stats_4h_ts
      WHERE chain = %L
        and vault_id = %L;

      INSERT INTO data_report.vault_stats_4h_ts (
        chain,
        vault_id,
        datetime,
        log_usd_owner_balance_histogram_1_1m_10b,
        owner_address_hll,
        usd_balance
      ) 
        with balance_4h_ts as (
          select owner_address,
              time_bucket_gapfill('4h', datetime) as datetime,
              locf(last(balance_after::numeric, datetime)) as balance
          from data_derived.erc20_owner_balance_diff_4h_ts
          -- make sure we select the previous snapshot to fill the graph
          where datetime between %L and %L
          and owner_address != evm_address_to_bytea('0x0000000000000000000000000000000000000000')
          and chain = %L
          and contract_address = %L
          group by 1,2
      ),
      new_vault_stats_4h_ts as (
        select 
            b.datetime,
            hyperloglog(262144, b.owner_address) as owner_address_hll,
            sum(
                (
                    (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
                )
                * vpt.avg_want_usd_value
            ) as usd_balance,
            histogram(log(
                (
                    (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
                )
                * vpt.avg_want_usd_value
            ), log(1), log(1000000), 10) as log_usd_owner_balance_histogram_1_1m_10b
        from balance_4h_ts as b
        left join data_derived.vault_ppfs_and_price_4h_ts vpt 
            on vpt.chain = %L
            and vpt.contract_address = %L
            and vpt.datetime = b.datetime
        where balance is not null
            and balance != 0
        group by b.datetime
      )
      select %L, %L, datetime,
        log_usd_owner_balance_histogram_1_1m_10b,
        owner_address_hll,
        usd_balance
      from new_vault_stats_4h_ts
      ;

      COMMIT;
    `,
      [
        // delete query filters
        contract.chain,
        contract.vault_id,
        // balance_4h_ts filters
        contract.first_datetime,
        contract.last_datetime,
        contract.chain,
        strAddressToPgBytea(contract.contract_address),
        // vault_stats_4h_ts join filters
        contract.chain,
        strAddressToPgBytea(contract.contract_address),
        // select raw values
        contract.chain,
        contract.vault_id,
      ]
    );
  }

  logger.info(`[DB] Running vacuum full on data_report.vault_stats_4h_ts`);
  await db_query(`
    VACUUM (FULL, ANALYZE) data_report.vault_stats_4h_ts;
  `);
}
