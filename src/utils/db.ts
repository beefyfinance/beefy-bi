import { Pool, PoolConfig, PoolClient } from "pg";
import pgf from "pg-format";
import * as pgcs from "pg-connection-string";
import { TIMESCALEDB_URL } from "./config";
import { allChainIds } from "../types/chain";
import { rootLogger } from "./logger";

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
export async function getPgPool(freshClient = false) {
  if (pool === null) {
    const config = pgcs.parse(TIMESCALEDB_URL) as any as PoolConfig;
    pool = new Pool(config);
  }
  if (freshClient) {
    const config = pgcs.parse(TIMESCALEDB_URL) as any as PoolConfig;
    return new Pool(config);
  }
  return pool;
}

// inject pg client as first argument
export function withPgClient<TArgs extends any[], TRes>(
  fn: (client: PoolClient, ...args: TArgs) => Promise<TRes>,
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

export async function db_transaction<TRes>(fn: (client: PoolClient) => Promise<TRes>) {
  const pgPool = await getPgPool(true);
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    const res = await fn(client);
    await client.query("COMMIT");
    return res;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function db_query<RowType>(sql: string, params: any[] = [], client: PoolClient | null = null): Promise<RowType[]> {
  logger.trace({ msg: "Executing query", data: { sql, params } });
  const pool = await getPgPool();
  const sql_w_params = pgf(sql, ...params);
  //console.log(sql_w_params);
  const useClient = client || pool;
  try {
    const res = await useClient.query(sql_w_params);
    const rows = res?.rows || null;
    logger.trace({ msg: "Query end", data: { sql, params, total: res?.rowCount } });
    return rows;
  } catch (error) {
    logger.error({ msg: "Query error", data: { sql, params, error } });
    throw error;
  }
}

export async function db_query_one<RowType>(sql: string, params: any[] = [], client: PoolClient | null = null): Promise<RowType | null> {
  const rows = await db_query<RowType>(sql, params, client);
  if (rows.length === 0) {
    return null;
  }
  return rows[0];
}

export function strAddressToPgBytea(evmAddress: string) {
  // 0xABC -> // \xABC
  return "\\x" + evmAddress.slice(2);
}

export function strArrToPgStrArr(strings: string[]) {
  return "{" + pgf.withArray("%L", strings) + "}";
}

export function pgStrArrToStrArr(pgArray: string[]) {
  return pgArray.map((s) => s.slice(1, -1).replace("''", "'"));
}

// postgresql don't have "create type/domain if not exists"
async function typeExists(typeName: string) {
  const res = await db_query_one(`SELECT * FROM pg_type WHERE typname = %L`, [typeName]);
  return res !== null;
}

export async function db_migrate() {
  logger.info({ msg: "Migrate begin" });
  // types
  if (!(await typeExists("chain_enum"))) {
    await db_query(`
        CREATE TYPE chain_enum AS ENUM ('ethereum');
    `);
  }
  for (const chain of allChainIds) {
    await db_query(`ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS %L`, [chain]);
  }

  if (!(await typeExists("evm_address_bytea"))) {
    await db_query(`
      CREATE DOMAIN evm_address_bytea AS BYTEA;
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
        CHECK (nullif(VALUE, 'NaN') is not null);
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

    -- Adapted from https://stackoverflow.com/a/49688529/2523414
    create or replace function jsonb_merge(CurrentData jsonb,newData jsonb)
      returns jsonb
      language sql
      immutable
      as $jsonb_merge_func$
      select case jsonb_typeof(CurrentData)
        when 'object' then case jsonb_typeof(newData)
          when 'object' then COALESCE((
            select    jsonb_object_agg(k, case
                        when e2.v is null then e1.v
                        when e1.v is null then e2.v
                        when e1.v = e2.v then e1.v 
                        else jsonb_merge(e1.v, e2.v)
                      end)
            from      jsonb_each(CurrentData) e1(k, v)
            full join jsonb_each(newData) e2(k, v) using (k)
          ), '{}'::jsonb)
          else newData
        end
        when 'array' then CurrentData || newData
        else newData
      end
      $jsonb_merge_func$;

      CREATE OR REPLACE FUNCTION jsonb_int_range_size(jsonb) RETURNS integer 
      AS $$
        select ($1->>'to')::integer - ($1->>'from')::integer + 1
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;

      CREATE OR REPLACE FUNCTION jsonb_int_ranges_size_sum(jsonb) RETURNS integer
      AS $$
        select coalesce(sum(jsonb_int_range_size(size)), 0)
        from (select * from jsonb_array_elements($1)) as range_size(size)
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;
  `);

  // token price registry to avoid manipulating and indexing strings on the other tables
  await db_query(`
    CREATE TABLE IF NOT EXISTS price_feed (
      price_feed_id serial PRIMARY KEY,
      -- unique price feed identifier
      feed_key varchar NOT NULL UNIQUE,
      external_id varchar NOT NULL, -- the id used by the feed
      -- what this price represents, can be used to get an usd value or a token conversion rate
      from_asset_key character varying NOT NULL,
      to_asset_key character varying NOT NULL,
      -- all relevant price feed data: eol, etc
      price_feed_data jsonb NOT NULL
    );
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS product (
      product_id serial PRIMARY KEY,

      -- the product unique external key
      product_key varchar UNIQUE NOT NULL,

      chain chain_enum NOT NULL,
      
      -- product price feed to get usd value
      -- this should reflect the investement balance currency
      price_feed_id integer not null references price_feed(price_feed_id),
      
      -- all relevant product infos, addresses, type, etc
      product_data jsonb NOT NULL
    );
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS investor (
      investor_id serial PRIMARY KEY,
      address evm_address_bytea NOT NULL UNIQUE,
      investor_data jsonb NOT NULL -- all relevant investor infos
    );
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS investment_balance_ts (
      datetime timestamptz not null,

      -- some chains have the same timestamp for distinct block numbers so we need to save the block number to the distinct key
      block_number integer not null,

      -- whatever contract the user is invested with
      product_id integer not null references product(product_id),
      investor_id integer not null references investor(investor_id),

      -- the investment details
      balance evm_decimal_256 not null, -- with decimals applied

      -- some debug info to help us understand how we got this data
      investment_data jsonb not null -- chain, block_number, transaction hash, transaction fees, etc
    );
    CREATE UNIQUE INDEX IF NOT EXISTS investment_balance_ts_uniq ON investment_balance_ts(product_id, investor_id, block_number, datetime);

    SELECT create_hypertable(
      relation => 'investment_balance_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days',
      if_not_exists => true
    );
  `);

  // price data
  await db_query(`
    CREATE TABLE IF NOT EXISTS price_ts (
      datetime TIMESTAMPTZ NOT NULL,

      price_feed_id integer not null references price_feed(price_feed_id),

      -- we may want to specify a block number to resolve duplicates
      -- if the prices do not comes from a chain, we put the timestamp in here
      block_number integer not null,

      price evm_decimal_256 not null,

      -- some debug info to help us understand how we got this data
      price_data jsonb not null -- chain, transaction hash, transaction fees, etc
    );

    -- make sure we don't create a unique index on a null value because all nulls are considered different
    CREATE UNIQUE INDEX IF NOT EXISTS price_ts_wi_chain_uniq ON price_ts(price_feed_id, block_number, datetime);

    SELECT create_hypertable(
      relation => 'price_ts',
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '7 days', 
      if_not_exists => true
    );
  `);

  // a table to store which data we already imported and which range needs to be retried
  // we need this because if there is no data on some date range, maybe we already fetched it and there is no datas
  await db_query(`
    CREATE TABLE IF NOT EXISTS import_state (
      import_key character varying PRIMARY KEY,
      import_data jsonb NOT NULL
    );
  `);

  logger.info({ msg: "Migrate done" });
}
