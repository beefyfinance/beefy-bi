import { Pool, PoolConfig, PoolClient } from "pg";
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
    octet_length('0x2BdfBd329984Cf0DC9027734681A16f542cF3bB4') as str_addr_size,
    (select typlen from pg_type where oid = 'bigint'::regtype::oid) as bigint_addr_size,
    (select typlen from pg_type where oid = 'int'::regtype::oid) as int_addr_size
    ;
    
 bytea_addr_size | str_addr_size | bigint_addr_size | int_addr_size 
-----------------+---------------+------------------+---------------
              20 |            42 |                8 |             4

(1 row)
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
  params: any[] = [],
  client: PoolClient | null = null
): Promise<RowType[]> {
  logger.debug(`Executing query: ${sql}, params: ${params}`);
  const pool = await getPgPool();
  const sql_w_params = pgf(sql, ...params);
  //console.log(sql_w_params);
  const useClient = client || pool;
  const res = await useClient.query(sql_w_params);
  const rows = res?.rows || null;
  logger.debug(`Got ${res?.rowCount} for query: ${sql}, params ${params}`);
  return rows;
}

export async function db_query_one<RowType>(
  sql: string,
  params: any[] = [],
  client: PoolClient | null = null
): Promise<RowType | null> {
  const rows = await db_query<RowType>(sql, params, client);
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
  const res = await db_query_one(`SELECT * FROM pg_type WHERE typname = %L`, [typeName]);
  return res !== null;
}

// avoid error ERROR:  cannot change configuration on already compressed chunks
// on alter table set compression
async function isCompressionEnabled(hyperTableSchema: string, hypertableName: string) {
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
      CREATE DOMAIN evm_address AS BYTEA;
    `);
  }

  if (!(await typeExists("uint_256"))) {
    await db_query(`
      CREATE DOMAIN uint_256 
        AS NUMERIC
        CHECK (VALUE >= 0 AND VALUE < 2^256)
        CHECK (SCALE(VALUE) = 0)
    `);
  }

  if (!(await typeExists("int_256"))) {
    await db_query(`
      CREATE DOMAIN int_256 
        AS NUMERIC
        CHECK (VALUE >= -2^255 AND VALUE < (2^255)-1)
        CHECK (SCALE(VALUE) = 0)
    `);
  }

  if (!(await typeExists("evm_decimal_256"))) {
    await db_query(`
      CREATE DOMAIN evm_decimal_256 
        AS NUMERIC(78, 24) -- 24 is the max decimals in current addressbook
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

      CREATE OR REPLACE FUNCTION array_unique_union(a ANYARRAY, b ANYARRAY)
      RETURNS ANYARRAY AS
      $$
        SELECT array_agg(distinct x)
        FROM (
          SELECT unnest(a) as x
          UNION ALL
          SELECT unnest(b) as x
        ) AS u
      $$ LANGUAGE SQL IMMUTABLE;

      create or replace function intarray_sum_elements(int[], int[])
      returns int[] language sql immutable as $$
          select array_agg(coalesce(a, 0)+ b)
          from unnest($1, $2) as u(a, b)
      $$;
      create or replace aggregate intarray_sum_elements_agg(integer[]) (
          sfunc = intarray_sum_elements,
          stype = int[]
      );
  `);

  if (!(await typeExists("evm_trait_erc20"))) {
    await db_query(`
      CREATE TYPE evm_trait_erc20 AS (
          decimals integer not null CHECK (decimals >= 0 AND decimals <= 78),
          name character varying not null,
      );
    `);
  }

  await db_query(`
    CREATE TABLE IF NOT EXISTS evm_address (
      evm_address_id serial PRIMARY KEY,
      chain chain_enum NOT NULL,
      address evm_address NOT NULL,
      creation_datetime TIMESTAMPTZ NOT NULL,
      trait_erc20 evm_trait_erc20,
      metadata jsonb NOT NULL
    );
    CREATE UNIQUE INDEX evm_address_uniq ON evm_address(chain, address);
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS evm_transaction (
      evm_transaction_id serial PRIMARY KEY,
      chain chain_enum NOT NULL,
      hash bytea NOT NULL,
      block_number integer not null,
      block_datetime TIMESTAMPTZ NOT NULL
    );
    CREATE UNIQUE INDEX evm_transaction_uniq ON evm_address(chain, hash);
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS vault_shares_change_ts (
      datetime timestamptz not null,
      evm_transaction_id integer not null references evm_transaction(evm_transaction_id),
      owner_evm_address_id integer not null references evm_address(evm_address_id),
      vault_evm_address_id integer not null references evm_address(evm_address_id),

      -- all numeric fields have decimals applied
      shares_balance_diff evm_decimal_256 not null,
      shares_balance_after evm_decimal_256 null, -- can be null if we can't query the archive node
    );
    CREATE UNIQUE INDEX vault_shares_change_ts_uniq ON vault_shares_change_ts(owner_evm_address_id, vault_evm_address_id, evm_transaction_id);

    SELECT create_hypertable(
      relation => 'vault_shares_change_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days',
      if_not_exists => true
    );
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS vault_to_underlying_rate_ts (
      datetime timestamptz not null,
      vault_evm_address_id integer not null references evm_address(evm_address_id),

      -- all numeric fields have decimals applied
      shares_to_underlying_rate evm_decimal_256 not null,
      shares_to_usd_rate evm_decimal_256 not null,
    );
    CREATE UNIQUE INDEX vault_to_underlying_rate_ts_uniq ON vault_to_underlying_rate_ts(vault_evm_address_id, datetime);

    SELECT create_hypertable(
      relation => 'vault_to_underlying_rate_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days',
      if_not_exists => true
    );
  `);

  // token price
  await db_query(`
    CREATE TABLE IF NOT EXISTS asset_price_ts (
      datetime TIMESTAMPTZ NOT NULL,
      asset_key varchar NOT NULL,
      usd_value double precision not null
    );
    CREATE UNIQUE INDEX asset_price_ts_uniq ON asset_price_ts(asset_key, datetime);
    SELECT create_hypertable(
      relation => 'asset_price_ts',
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '7 days', 
      if_not_exists => true
    );
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_vault (
      vault_id serial PRIMARY KEY,
      vault_key varchar NOT NULL,
      contract_evm_address_id integer not null references evm_address(evm_address_id),
      underlying_evm_address_id integer not null references evm_address(evm_address_id),
      end_of_life boolean not null,
      has_erc20_shares_token boolean not null,
      last_sync_datetime TIMESTAMPTZ NOT NULL,
      assets_oracle_id varchar[] not null,
    );

    CREATE UNIQUE INDEX beefy_vault_uniq ON beefy_vault(contract_evm_address_id);
  `);

  /**
   
    evm_address:
      evm_address_id: serial
      chain: chain_enum
      address: bytea
      metadata: jsonb (
          token decimals, 
          underlying assets, 
          role (erc20|vault|wallet| ...), 
          type (user|contract|vault|boost), 
          protocol, 
          url, 
          ingestion fields (min ingested block, max ingested block), 
          etc
      )
      UNIQUE INDEX: (chain, address)

    evm_transaction:
      evm_transaction_id: serial
      chain: chain_enum
      hash: bytea
      block_number: integer
      block_timestamp: datetime

      -- handle upserts
      unique index: (chain, hash)

    vault_shares_balance_change: 
      datetime timestamptz not null,
      evm_transaction_id integer not null references evm_transaction(evm_transaction_id),
      owner_evm_address_id integer not null references evm_address(evm_address_id),
      vault_evm_address_id integer not null references evm_address(evm_address_id),
      
      // all numeric fields have decimals applied
      shares_balance_diff numeric not null,
      shares_balance_after numeric null, // can be null if we can't query the archive node

      // used to upsert and to find by owner_address_id
      UNIQUE INDEX (owner_evm_address_id, vault_evm_address_id, evm_transaction_id (nullable))
    
    
    vault_to_underlying_rate:
      datetime timestamptz not null,
      vault_evm_address_id integer not null references evm_address(evm_address_id),
      
      // shared to underlying can be multiplied with shares_amounts directly so they are not ppfs directly
      // all these can be null if we can't query the archive node
      shares_to_underlying_rate numeric null

      shares_to_token_breakdown_rate numeric[] null


    
Storage design decision:
- separate amount of token and price of token: allow to display data when price is not available and avoid updating rows
- have a daily/weekly token amount checkpoint for all users: allow to display current amount of token of a user without looking at an unbounded history
- separate vault shares price from other prices (partition?): reduce the amount of data pages to be loaded
- have raw data tables with all debug data separated from smaller view tables (no transaction hash), where data fits in memory so query is faster 


SharesAmountChange -> 
  chain -> params.chain
  vault_id -> params.vault_id
  investor_address -> event.from / event.to
  block_number -> event.block_number
  block_timestamp -> getBlock(event.block).timestamp
  transaction_hash -> event.transaction_hash

  shares_diff_amount -> event.value
  shares_balance_after -> balanceOf(blockTag: event.block)

  sharedToUnderlyingRate -> pricePerFullShare()
  underlying_balance_diff -> shares_diff_amount * sharedToUnderlyingRate
  underlying_balance_after -> shares_balance_after * sharedToUnderlyingRate

  investment_usd_value -> price_feed.getPrice(underlying, datetime)

Price ->
  token_a_id -> params.token_a_id
  token_b_id -> params.token_b_id
  datetime -> feed.datetime
  rate -> feed.rate



TimeRange: blockNumberRange | blockNumber[] | dateRange

price_feed_connector:
  - fetch_price(Token[], TimeRange) -> Stream<Price>
  - live_price(Token[]) -> Stream<Price> // maybe optional

protocol_connector (chain: Chain):
  - fetch_vault_list() -> Stream<Vault>

  - fetch_investment_changes(Vault[], TimeRange) -> Stream<SharesAmountChange>
  - fetch_shares_to_underlying_rate(Vault[], TimeRange) -> Stream<SharesToUnderlyingRate>
  - subscribe_to_shares_amount_changes(Vault[]) -> Stream<SharesAmountChange>

  - fetch_underlying_breakdown(Vault[], TimeRange) -> Stream<VaultUnderlyingBreakdown>


vault
vault_shares_amount_change
vault_shares_to_underlying_rate

price
  - partition: vault_shares_to_underlying_rate (ppfs) 
  - partition: vault_underlying_price
  - partition: token_price


 */
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
  await db_query(`
    SELECT add_continuous_aggregate_policy('data_derived.vault_ppfs_4h_ts',
      start_offset => INTERVAL '1 month',
      end_offset => INTERVAL '4 hour',
      schedule_interval => INTERVAL '4 hour',
      if_not_exists => true
    );
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
  await db_query(`
    SELECT add_continuous_aggregate_policy('data_derived.oracle_price_4h_ts',
      start_offset => INTERVAL '1 month',
      end_offset => INTERVAL '4 hour',
      schedule_interval => INTERVAL '4 hour',
      if_not_exists => true
    );
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

      CREATE INDEX IF NOT EXISTS idx_owner_eobd4h ON data_derived.erc20_owner_balance_diff_4h_ts(owner_address);
  `);
  await db_query(`
    SELECT add_continuous_aggregate_policy('data_derived.erc20_owner_balance_diff_4h_ts',
      start_offset => INTERVAL '1 month',
      end_offset => INTERVAL '4 hour',
      schedule_interval => INTERVAL '4 hour',
      if_not_exists => true
    );
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
  await db_query(`
    SELECT add_continuous_aggregate_policy('data_derived.erc20_contract_balance_diff_4h_ts',
      start_offset => INTERVAL '1 month',
      end_offset => INTERVAL '4 hour',
      schedule_interval => INTERVAL '4 hour',
      if_not_exists => true
    );
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
    -- index to speed up vault stats reloading (90s -> 12s)
    CREATE INDEX IF NOT EXISTS idx_chain_contract_datetime_dt_vpp4h ON data_derived.vault_ppfs_and_price_4h_ts (chain, contract_address, datetime);
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
      owner_address_hll hyperloglog not null,
      balance_usd_value double precision, -- prices can be null
      balance_want_value numeric,
      balance_usd_ranges_counts integer[]
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

  // continuous aggregates: chain stats
  // we need this because rollups on hll are expensive
  await db_query(`
  CREATE MATERIALIZED VIEW IF NOT EXISTS data_report.chain_stats_4h_ts WITH (timescaledb.continuous)
    AS 
    select chain,
      time_bucket('4h', datetime) as datetime,
      rollup(owner_address_hll) as owner_address_hll,
      distinct_count(rollup(owner_address_hll)) as approx_owner_address_count,
      sum(balance_usd_value) as balance_usd_value,
      sum(balance_want_value) as balance_want_value,
      intarray_sum_elements_agg(balance_usd_ranges_counts) as balance_usd_ranges_counts
    from data_report.vault_stats_4h_ts
    group by 1,2;
  `);
  await db_query(`
    SELECT add_continuous_aggregate_policy('data_report.chain_stats_4h_ts',
      start_offset => INTERVAL '1 month',
      end_offset => INTERVAL '4 hour',
      schedule_interval => INTERVAL '4 hour',
      if_not_exists => true
    );
  `);

  // continuous aggregates: all chains stats
  // we need this because rollups on hll are expensive
  await db_query(`
  CREATE MATERIALIZED VIEW IF NOT EXISTS data_report.all_stats_4h_ts WITH (timescaledb.continuous)
    AS 
    select
      time_bucket('4h', datetime) as datetime,
      rollup(owner_address_hll) as owner_address_hll,
      distinct_count(rollup(owner_address_hll)) as approx_owner_address_count,
      sum(balance_usd_value) as balance_usd_value,
      sum(balance_want_value) as balance_want_value,
      intarray_sum_elements_agg(balance_usd_ranges_counts) as balance_usd_ranges_counts
    from data_report.vault_stats_4h_ts
    group by 1;
  `);
  await db_query(`
    SELECT add_continuous_aggregate_policy('data_report.all_stats_4h_ts',
      start_offset => INTERVAL '1 month',
      end_offset => INTERVAL '4 hour',
      schedule_interval => INTERVAL '4 hour',
      if_not_exists => true
    );
  `);

  // vault harvests table
  await db_query(`
    CREATE TABLE IF NOT EXISTS data_raw.vault_harvest_1d_ts (
      chain chain_enum NOT NULL,
      vault_id varchar NOT NULL,
      datetime TIMESTAMPTZ NOT NULL check (datetime = date_trunc('day', datetime)), -- ensure that datetime is at midnight
      strategy_address evm_address NOT NULL,
      harvest_count integer NOT NULL,
      caller_wnative_amount numeric not null,
      strategist_wnative_amount numeric not null,
      beefy_wnative_amount numeric not null,
      compound_wnative_amount numeric not null,
      ukn_wnative_amount numeric not null
    );
    SELECT create_hypertable(
      relation => 'data_raw.vault_harvest_1d_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days', 
      if_not_exists => true
    );
  `);
}

// this is kind of a continuous aggregate
// but we need a gapfill which is way faster to execute on a vault by vault basis
export async function rebuildVaultStatsReportTable() {
  const pool = await getPgPool();
  const client = await pool.connect();
  try {
    // we select the min and max date to feed the gapfill function
    const contracts = await db_query<{
      chain: string;
      contract_address: string;
      vault_id: string;
      first_datetime: Date;
      last_datetime: Date;
    }>(
      `
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
      `,
      [],
      client
    );

    // create a target table so we don't lock the main one
    await db_query(`CREATE TEMPORARY TABLE vault_stats_4h_ts_import (LIKE data_report.vault_stats_4h_ts);`, [], client);

    // prepare the complex query to avoid ~1s of JIT compilation each time
    await db_query(
      `
        -- chain, vault_id, contract_address, first datetime, last datetime
        PREPARE vault_stats_inserts (chain_enum, text, evm_address, timestamptz, timestamptz) AS
          INSERT INTO vault_stats_4h_ts_import (
            chain,
            vault_id,
            datetime,
            owner_address_hll,
            balance_usd_value,
            balance_want_value,
            balance_usd_ranges_counts
          ) (
              with balance_4h_ts as (
                select owner_address,
                    time_bucket_gapfill('4h', datetime) as datetime,
                    locf(last(balance_after::numeric, datetime)) as balance
                from data_derived.erc20_owner_balance_diff_4h_ts
                -- make sure we select the previous snapshot to fill the graph
                where datetime between $4 and $5
                and owner_address != evm_address_to_bytea('0x0000000000000000000000000000000000000000')
                and chain = $1
                and contract_address = $3
                group by 1,2
            ),
            balance_4h_ts_with_usd_price as materialized ( -- prevent expensive late group by plan
              select 
                  b.datetime, b.owner_address,
                  (
                      (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
                  )
                  * vpt.avg_want_usd_value as balance_usd_value,
                  (
                    (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
                  ) as balance_want_value
              from balance_4h_ts as b
              left join data_derived.vault_ppfs_and_price_4h_ts vpt 
                  on vpt.chain = $1
                  and vpt.contract_address = $3
                  and vpt.datetime = b.datetime
              where b.balance is not null
                  and b.balance != 0
            ),
            new_vault_stats_4h_ts as (
              select 
                  datetime,
                  hyperloglog(262144, owner_address) as owner_address_hll,
                  sum(balance_usd_value) as balance_usd_value,
                  sum(balance_want_value) as balance_want_value,
                  intarray_sum_elements_agg(
                    case 
                      when balance_usd_value is null then null
                      when balance_usd_value < 0.0        then ARRAY[1,0,0,0,0,0,0,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value = 0.0        then ARRAY[0,1,0,0,0,0,0,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 10.0       then ARRAY[0,0,1,0,0,0,0,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 100.0      then ARRAY[0,0,0,1,0,0,0,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 1000.0     then ARRAY[0,0,0,0,1,0,0,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 5000.0     then ARRAY[0,0,0,0,0,1,0,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 10000.0    then ARRAY[0,0,0,0,0,0,1,0,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 25000.0    then ARRAY[0,0,0,0,0,0,0,1,0,0,0,0,0,0]::int[]
                      when balance_usd_value < 50000.0    then ARRAY[0,0,0,0,0,0,0,0,1,0,0,0,0,0]::int[]
                      when balance_usd_value < 100000.0   then ARRAY[0,0,0,0,0,0,0,0,0,1,0,0,0,0]::int[]
                      when balance_usd_value < 500000.0   then ARRAY[0,0,0,0,0,0,0,0,0,0,1,0,0,0]::int[]
                      when balance_usd_value < 1000000.0  then ARRAY[0,0,0,0,0,0,0,0,0,0,0,1,0,0]::int[]
                      when balance_usd_value < 10000000.0 then ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,1,0]::int[]
                                                          else ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,1]::int[]
                    end
                  ) as balance_usd_ranges_counts
              from balance_4h_ts_with_usd_price
              group by datetime
            )
            select $1, $2, datetime,
              owner_address_hll,
              balance_usd_value,
              coalesce(balance_want_value, 0) as balance_want_value,
              balance_usd_ranges_counts
            from new_vault_stats_4h_ts
          )
          RETURNING null -- node parser cannot comprehend the hyperloglog format
          ;
      `,
      [],
      client
    );

    for (const [idx, contract] of contracts.entries()) {
      logger.info(
        `[DB] Refreshing vault stats for vault ${contract.chain}:${
          contract.vault_id
        } between ${contract.first_datetime.toISOString()} and ${contract.last_datetime.toISOString()} (${idx}/${
          contracts.length
        })`
      );
      try {
        await db_query(
          `EXECUTE vault_stats_inserts(%L, %L, %L, %L, %L);`,
          [
            contract.chain,
            contract.vault_id,
            strAddressToPgBytea(contract.contract_address),
            contract.first_datetime,
            contract.last_datetime,
          ],
          client
        );
      } catch (e) {
        logger.error(
          `[DB] Could not refresh vault stats for ${contract.chain}:${contract.vault_id}:${contract.contract_address}`
        );
        console.log(e);
      }

      logger.info(
        `[DB] Refresh DONE for vault stats for vault ${contract.chain}:${contract.vault_id} (${idx}/${contracts.length})`
      );
    }

    logger.info(`[DB] Transfering imported data to the data_report.vault_stats_4h_ts table`);
    // now transfer to the main table
    await db_query(
      `
        BEGIN;

          TRUNCATE TABLE data_report.vault_stats_4h_ts;
          INSERT INTO data_report.vault_stats_4h_ts (SELECT * FROM vault_stats_4h_ts_import);

        COMMIT;
      `,
      [],
      client
    );
  } finally {
    client.release();
  }

  logger.info(`[DB] Running vacuum full on data_report.vault_stats_4h_ts`);
  await db_query(`
    VACUUM (FULL, ANALYZE) data_report.vault_stats_4h_ts;
  `);

  logger.info(`[DB] Refreshing continuous aggregates`);
  await db_query(`
    CALL refresh_continuous_aggregate('data_report.chain_stats_4h_ts', '2018-01-01', now());
    CALL refresh_continuous_aggregate('data_report.all_stats_4h_ts', '2018-01-01', now());
  `);
  logger.info(`[DB] Refreshing vault stats DONE`);
}
