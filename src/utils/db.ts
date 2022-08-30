import { Pool, PoolConfig, PoolClient } from "pg";
import pgf from "pg-format";
import * as pgcs from "pg-connection-string";
import { TIMESCALEDB_URL } from "./config";
import { normalizeAddress } from "./ethers";
import { allChainIds } from "../types/chain";
import { rootLogger } from "./logger2";

const logger = rootLogger.child({ module: "db", component: "query" });

/**
 * evm_address: 
 *  It's a bit more difficult to use but half the size using bytea instead of string
 *  also, there is no case weirdness with bytea
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

// inject pg client as first argument
export function withPgClient<TArgs extends any[], TRes>(
  fn: (client: PoolClient, ...args: TArgs) => Promise<TRes>
): (...args: TArgs) => Promise<TRes> {
  return async (...args: TArgs) => {
    const pgPool = await getPgPool();
    const client = await pgPool.connect();
    let res: TRes;
    try {
      res = await fn(client, ...args);
    } finally {
      client.release();
    }
    return res;
  };
}

export async function db_query<RowType>(
  sql: string,
  params: any[] = [],
  client: PoolClient | null = null
): Promise<RowType[]> {
  logger.debug({ msg: "Executing query", data: { sql, params } });
  const pool = await getPgPool();
  const sql_w_params = pgf(sql, ...params);
  //console.log(sql_w_params);
  const useClient = client || pool;
  const res = await useClient.query(sql_w_params);
  const rows = res?.rows || null;
  logger.trace({ msg: "Query end", data: { sql, params, total: res?.rowCount } });
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

  if (!(await typeExists("evm_trx_hash"))) {
    await db_query(`
      CREATE DOMAIN evm_trx_hash AS BYTEA;
    `);
  }

  if (!(await typeExists("evm_decimal_256"))) {
    await db_query(`
      CREATE DOMAIN evm_decimal_256 
        -- 24 is the max decimals in current addressbook, might change in the future
        -- 100 is the maximum number of digits stored, not the reserved space
        AS NUMERIC(100, 24)
        CHECK (VALUE is not 'NaN')
    `);
  }

  // helper function
  await db_query(`
      CREATE OR REPLACE FUNCTION bytea_to_hexstr(bytea) RETURNS character varying 
        AS $$
          SELECT '0x' || encode($1::bytea, 'hex')
        $$
        LANGUAGE SQL
        IMMUTABLE
        RETURNS NULL ON NULL INPUT;

    CREATE OR REPLACE FUNCTION hexstr_to_bytea(varchar) RETURNS bytea 
      AS $$
        select decode(substring($1 ,3), 'hex')
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;
  `);

  // an address and transaction table to avoid bloating the tables and indices
  await db_query(`
    CREATE TABLE IF NOT EXISTS evm_address (
      evm_address_id serial PRIMARY KEY,
      chain chain_enum NOT NULL,
      address evm_address NOT NULL,
      metadata jsonb NOT NULL
    );
    CREATE UNIQUE INDEX evm_address_uniq ON evm_address(chain, address);
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS evm_transaction (
      evm_transaction_id serial PRIMARY KEY,
      chain chain_enum NOT NULL,
      hash evm_trx_hash NOT NULL,
      block_number integer not null,
      block_datetime TIMESTAMPTZ NOT NULL
    );
    CREATE UNIQUE INDEX evm_transaction_uniq ON evm_address(chain, hash);
  `);

  // stores vault transfers of shares
  // may represent a virtual vault (that doesn't give a token back)
  // in this case we have a 1:1 mapping between the underlying and the share
  await db_query(`
    CREATE TABLE IF NOT EXISTS vault_shares_transfer_ts (
      datetime timestamptz not null,
      evm_transaction_id integer not null references evm_transaction(evm_transaction_id),
      owner_evm_address_id integer not null references evm_address(evm_address_id),
      vault_evm_address_id integer not null references evm_address(evm_address_id),

      -- all numeric fields have decimals applied
      shares_balance_diff evm_decimal_256 not null,
      shares_balance_after evm_decimal_256 null, -- can be null if we can't query the archive node
    );
    CREATE UNIQUE INDEX vault_shares_transfer_ts_uniq ON vault_shares_transfer_ts(owner_evm_address_id, vault_evm_address_id, evm_transaction_id);

    SELECT create_hypertable(
      relation => 'vault_shares_transfer', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days',
      if_not_exists => true
    );
  `);

  // exchange rate betwwen a share and the underlying
  // also contains the usd rate
  await db_query(`
    CREATE TABLE IF NOT EXISTS vault_to_underlying_rate_ts (
      datetime timestamptz not null,
      vault_evm_address_id integer not null references evm_address(evm_address_id),

      -- all numeric fields have decimals applied
      shares_to_underlying_rate evm_decimal_256 not null
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
      price_feed_key varchar NOT NULL,
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
      assets_price_feed_keys varchar[] not null
    );

    CREATE UNIQUE INDEX beefy_vault_uniq ON beefy_vault(contract_evm_address_id);
  `);

  // a table to store which vault we already imported and which range needs to be retried
  await db_query(`
    CREATE TABLE IF NOT EXISTS import_status (
      import_key varchar NOT NULL,

      -- either blocknumber range or timestamp range
      imported_range int4range NOT NULL,
      
      -- those are ranges with errors that need to be retried
      ranges_to_retry int4range[] NOT NULL,
    );
  `);

  /**
   

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
}
