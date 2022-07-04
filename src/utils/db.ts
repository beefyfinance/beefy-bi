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

    CREATE OR REPLACE FUNCTION evm_address_to_bytea(varchar) RETURNS bytea 
      AS $$
        select decode(substring($1 ,3), 'hex')
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

  // balance diff table
  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_raw.erc20_balance_diff_ts (
      chain chain_enum NOT NULL,
      contract_address evm_address NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      investor_address evm_address not null,
      balance_diff int_256 not null
    );
    SELECT create_hypertable(
      relation => 'beefy_raw.erc20_balance_diff_ts', 
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '7 days', 
      if_not_exists => true
    );
  `);

  if (!(await isCompressionEnabled("beefy_raw", "erc20_balance_diff_ts"))) {
    await db_query(`
      ALTER TABLE beefy_raw.erc20_balance_diff_ts SET (
        timescaledb.compress, 
        timescaledb.compress_segmentby = 'chain, contract_address',
        timescaledb.compress_orderby = 'datetime DESC'
      );
      SELECT add_compression_policy(
        hypertable => 'beefy_raw.erc20_balance_diff_ts', 
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

  // oracle price
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

  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_raw.vault (
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
    CREATE MATERIALIZED VIEW IF NOT EXISTS beefy_derived.vault_ppfs_4h_ts WITH (timescaledb.continuous)
      AS select ts.chain, ts.contract_address, 
          time_bucket('4h', ts.datetime) as datetime, 
          avg(ts.ppfs) as avg_ppfs
      from beefy_raw.vault_ppfs_ts ts
      group by 1,2,3;
  `);
  // continuous aggregates: prices
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS beefy_derived.oracle_price_4h_ts WITH (timescaledb.continuous)
      AS select oracle_id,
          time_bucket('4h', datetime) as datetime, 
          avg(usd_value) as avg_usd_value
      from beefy_raw.oracle_price_ts
      group by 1,2;
  `);
  // continuous aggregates: transfer diffs
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS beefy_derived.erc20_balance_diff_4h_ts WITH (timescaledb.continuous)
      AS select chain, contract_address, investor_address, 
          time_bucket('4h', datetime) as datetime, 
          sum(balance_diff) as balance_diff,
          sum(balance_diff) filter(where balance_diff > 0) as deposit_diff,
          sum(balance_diff) filter(where balance_diff < 0) as withdraw_diff,
          count(*) as trx_count,
          count(*) filter (where balance_diff > 0) as deposit_count,
          count(*) filter (where balance_diff < 0) as withdraw_count
      from beefy_raw.erc20_balance_diff_ts
      group by 1,2,3,4;
  `);

  // helper materialized view
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS beefy_derived.vault_ppfs_and_price_4h_ts as 
      with vault_scope as (
          select chain, token_address, vault_id, want_price_oracle_id, want_decimals
          from beefy_raw.vault
      ), 
      want_prices_ts as (
          select oracle_id, datetime, avg_usd_value
          from beefy_derived.oracle_price_4h_ts
          where oracle_id in (
              select distinct scope.want_price_oracle_id 
              from vault_scope scope
          )
      ),
      ppfs_ts as (
          select chain, contract_address, datetime, avg_ppfs
          from beefy_derived.vault_ppfs_4h_ts ts
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
    CREATE INDEX IF NOT EXISTS idx_chain_vpp4h ON beefy_derived.vault_ppfs_and_price_4h_ts (chain);
    CREATE INDEX IF NOT EXISTS idx_address_vpp4h ON beefy_derived.vault_ppfs_and_price_4h_ts (contract_address);
    CREATE INDEX IF NOT EXISTS idx_datetime_vpp4h ON beefy_derived.vault_ppfs_and_price_4h_ts (datetime);
    -- index to speed up investor dashboard (5s -> 100ms)
    CREATE INDEX IF NOT EXISTS idx_chain_vault_dt_vpp4h ON beefy_derived.vault_ppfs_and_price_4h_ts (chain, vault_id, datetime);
  `);

  // build a full report table with balance diffs every 4h and balance snapshots every 3d
  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts (
      chain chain_enum NOT NULL,
      vault_id varchar NOT NULL,
      investor_address evm_address NOT NULL,
      datetime TIMESTAMPTZ NOT NULL,
      token_balance int_256 not null,
      balance_usd_value double precision, -- some usd value may not be available
      balance_diff_usd_value double precision, 
      deposit_diff_usd_value double precision,
      withdraw_diff_usd_value double precision,
      balance_diff int_256 not null,
      deposit_diff int_256 not null,
      withdraw_diff int_256 not null,
      trx_count integer not null,
      deposit_count integer not null,
      withdraw_count integer not null
    );
    SELECT create_hypertable(
      relation => 'beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts', 
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '14 days', 
      if_not_exists => true
    );

    CREATE INDEX IF NOT EXISTS idx_chain_vib4hs3d ON beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts (chain);
    CREATE INDEX IF NOT EXISTS idx_investor_vib4hs3d ON beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts (investor_address);
    CREATE INDEX IF NOT EXISTS idx_vault_vib4hs3d ON beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts (vault_id);
  `);
}

// this is kind of a continuous aggregate
// there currently is no way to do continuous aggregates with window functions (cumulative sum to get the balance)
// so we rebuild this table as needed
// it needs to be an hypertable if we want to use ts functions like time_bucket_gapfill so materialized view are not a good fit
export async function rebuildBalanceReportTable() {
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
      FROM beefy_derived.erc20_balance_diff_4h_ts
      GROUP BY 1,2
    )
    select dates.chain, format_evm_address(dates.contract_address) as contract_address,
      dates.first_datetime, dates.last_datetime, vault.vault_id
    from contract_diff_dates dates
    join beefy_raw.vault vault on (dates.chain = vault.chain and dates.contract_address = vault.token_address)
    order by dates.chain, vault.vault_id
  `);

  for (const [idx, contract] of Object.entries(contracts)) {
    logger.info(
      `[DB] Refreshing data for vault ${contract.chain}:${contract.vault_id} (${idx}/${contracts.length})`
    );
    await db_query(
      `
      BEGIN;
      DELETE FROM beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts
      WHERE chain = %L
        and vault_id = %L;

      INSERT INTO beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts (
        chain,
        vault_id,
        investor_address,
        datetime,
        token_balance,
        balance_diff,
        deposit_diff,
        withdraw_diff,
        trx_count,
        deposit_count,
        withdraw_count,
        balance_usd_value,
        balance_diff_usd_value,
        deposit_diff_usd_value,
        withdraw_diff_usd_value
      ) 
      with
      balance_ts as (
          SELECT investor_address, datetime,
              balance_diff, trx_count, deposit_diff, withdraw_diff, deposit_count, withdraw_count,
              sum(balance_diff) over (
                  partition by investor_address
                  order by datetime asc
              ) as balance
          FROM beefy_derived.erc20_balance_diff_4h_ts
          where chain = %L
            and contract_address = %L
      ),
      -- create a balance snapshot every now and then to allow for analysis with filters on date
      balance_snapshots_full_hist_ts as (
          select investor_address, 
              time_bucket_gapfill('3d', datetime) as datetime,
              locf((array_agg(balance order by datetime desc))[1]) as balance_snapshot,
              lag(locf((array_agg(balance order by datetime desc))[1])) over (
                  partition by investor_address
                  order by time_bucket_gapfill('3d', datetime)
              ) as prev_balance_snapshot
          from balance_ts
          where datetime between %L and %L
          group by 1,2
      ),
      balance_snapshots_ts as (
          select investor_address, 
          -- assign the snapshot to the end of the last period
          datetime + interval '3 days' as datetime, 
          balance_snapshot
          from balance_snapshots_full_hist_ts
          where 
              -- remove balances before the first investment
              balance_snapshot is not null 
              -- remove rows after full exit of this user
              and not (prev_balance_snapshot = 0 and balance_snapshot = 0)
      ),
      balance_diff_with_snaps_ts as (
          -- now that we have balance snapshots, we can intertwine them with diffs to get a diff history with some snapshots
          select 
              coalesce(b.investor_address, bs.investor_address) as investor_address, 
              coalesce(b.datetime, bs.datetime) as datetime, 
              coalesce(b.balance, bs.balance_snapshot) as balance,
              coalesce(b.balance_diff, 0) as balance_diff, 
              coalesce(b.deposit_diff, 0) as deposit_diff,
              coalesce(b.withdraw_diff, 0) as withdraw_diff,
              coalesce(b.trx_count, 0) as trx_count,
              coalesce(b.deposit_count, 0) as deposit_count,
              coalesce(b.withdraw_count, 0) as withdraw_count
          from balance_snapshots_ts bs
          full outer join balance_ts b 
              on b.investor_address = bs.investor_address
              and b.datetime = bs.datetime
      ),
      investor_metrics as (
        select 
            vpt.vault_id, bt.investor_address, bt.datetime, 
            bt.balance as token_balance, bt.balance_diff, bt.deposit_diff, bt.withdraw_diff, 
            bt.trx_count, bt.deposit_count, bt.withdraw_count,
            (
                (bt.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
            ) * vpt.avg_want_usd_value as balance_usd_value,
            (
                (bt.balance_diff::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
            ) * vpt.avg_want_usd_value as balance_diff_usd_value,
            (
                (bt.deposit_diff::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
            ) * vpt.avg_want_usd_value as deposit_diff_usd_value,
            (
                (bt.withdraw_diff::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
            ) * vpt.avg_want_usd_value as withdraw_diff_usd_value
        from balance_diff_with_snaps_ts as bt
        join beefy_derived.vault_ppfs_and_price_4h_ts as vpt 
            on vpt.chain = %L
            and vpt.vault_id = %L
            and vpt.datetime = bt.datetime
      )
      select %L as chain, vault_id, investor_address, datetime,
        token_balance, 
        balance_diff, deposit_diff, withdraw_diff, 
        trx_count, deposit_count, withdraw_count,
        balance_usd_value, balance_diff_usd_value, deposit_diff_usd_value, withdraw_diff_usd_value
      from investor_metrics;
      COMMIT;
    `,
      [
        // delete query filters
        contract.chain,
        contract.vault_id,
        // balance_ts filters
        contract.chain,
        strAddressToPgBytea(contract.contract_address),
        // balance_snapshots_full_hist_ts filters
        contract.first_datetime,
        contract.last_datetime,
        // investor_metrics filters
        contract.chain,
        contract.vault_id,
        // select raw values
        contract.chain,
      ]
    );
  }

  logger.info(
    `[DB] Running vacuum full on beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts`
  );
  await db_query(`
    VACUUM (FULL, ANALYZE) beefy_report.vault_investor_balance_diff_4h_snaps_3d_ts;
  `);
}
