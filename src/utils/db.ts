import { Client as PgClient, ClientConfig as PgClientConfig } from "pg";
import * as pgcs from "pg-connection-string";
import pgf from "pg-format";
import { allChainIds } from "../types/chain";
import { withTimeout } from "./async";
import { TIMESCALEDB_URL } from "./config";
import { LogInfos, mergeLogsInfos, rootLogger } from "./logger";

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
export type DbClient = PgClient;

let sharedClient: PgClient | null = null;
const appNameCounters: Record<string, number> = {};
export async function getPgClient({ appName = "beefy", freshClient = false }: { appName?: string; freshClient?: boolean }) {
  if (!appNameCounters[appName]) {
    appNameCounters[appName] = 0;
  }

  if (sharedClient === null) {
    appNameCounters[appName] += 1;
    const appNameToUse = appName + ":common:" + appNameCounters[appName];

    const pgUrl = TIMESCALEDB_URL;
    const config = pgcs.parse(pgUrl) as any as PgClientConfig;
    logger.trace({ msg: "Instanciating new pg client", data: { appNameToUse } });
    sharedClient = new PgClient({ ...config, application_name: appNameToUse });
    sharedClient.on("error", (err: any) => {
      logger.error({ msg: "Postgres client error", data: { err, appNameToUse } });
      logger.error(err);
    });
  }
  if (freshClient) {
    appNameCounters[appName] += 1;
    const appNameToUse = appName + ":fresh:" + appNameCounters[appName];

    const pgUrl = TIMESCALEDB_URL;
    const config = pgcs.parse(pgUrl) as any as PgClientConfig;
    logger.trace({ msg: "Instanciating new pg client", data: { appNameToUse } });
    return new PgClient({ ...config, application_name: appNameToUse });
  }

  return sharedClient;
}

// inject pg client as first argument
export function withPgClient<TArgs extends any[], TRes>(
  fn: (client: PgClient, ...args: TArgs) => Promise<TRes>,
  { appName, connectTimeoutMs = 10_000, logInfos }: { appName: string; connectTimeoutMs?: number; logInfos: LogInfos },
): (...args: TArgs) => Promise<TRes> {
  return async (...args: TArgs) => {
    const pgClient = await getPgClient({ appName, freshClient: false });
    let res: TRes;
    try {
      await withTimeout(() => pgClient.connect(), connectTimeoutMs, logInfos);
      res = await fn(pgClient, ...args);
    } finally {
      await pgClient.end();
    }
    return res;
  };
}

export async function db_transaction<TRes>(
  fn: (client: PgClient) => Promise<TRes>,
  {
    appName,
    connectTimeoutMs = 10_000,
    queryTimeoutMs = 10_000,
    logInfos,
  }: { appName: string; connectTimeoutMs?: number; queryTimeoutMs?: number; logInfos: LogInfos },
) {
  const pgClient = await getPgClient({ appName, freshClient: true });
  try {
    await withTimeout(() => pgClient.connect(), connectTimeoutMs, mergeLogsInfos(logInfos, { msg: "connect", data: { connectTimeoutMs } }));
    try {
      await pgClient.query("BEGIN");
      const res = await withTimeout(() => fn(pgClient), queryTimeoutMs, mergeLogsInfos(logInfos, { msg: "query", data: { queryTimeoutMs } }));
      await pgClient.query("COMMIT");
      return res;
    } catch (error) {
      await pgClient.query("ROLLBACK");
      throw error;
    }
  } finally {
    await pgClient.end();
  }
}

export async function db_query<RowType>(sql: string, params: any[] = [], client: PgClient | null = null): Promise<RowType[]> {
  logger.trace({ msg: "Executing query", data: { sql, params } });
  let useClient: PgClient | null = client;
  if (useClient === null) {
    const pool = await getPgClient({ freshClient: false });
    useClient = pool;
  }
  const sql_w_params = pgf(sql, ...params);
  //console.log(sql_w_params);
  try {
    const res = await useClient.query(sql_w_params);
    const rows = res?.rows || null;
    logger.trace({ msg: "Query end", data: { sql, params, total: res?.rowCount } });
    return rows;
  } catch (error) {
    logger.error({ msg: "Query error", data: { sql, params, error } });
    logger.error(error);
    throw error;
  }
}

export async function db_query_one<RowType>(sql: string, params: any[] = [], client: PgClient | null = null): Promise<RowType | null> {
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

      
      CREATE OR REPLACE FUNCTION jsonb_date_range_size(jsonb) RETURNS interval 
      AS $$
        select ($1->>'to')::timestamptz - ($1->>'from')::timestamptz + interval '1 ms'
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;


      CREATE OR REPLACE FUNCTION jsonb_date_ranges_size_sum(jsonb) RETURNS interval
      AS $$
        select coalesce(sum(jsonb_date_range_size(size)), '0 ms'::interval)
        from (select * from jsonb_array_elements($1)) as range_size(size)
      $$
      LANGUAGE SQL
      IMMUTABLE
      RETURNS NULL ON NULL INPUT;


      CREATE OR REPLACE FUNCTION GapFillInternal( 
        s anyelement, 
        v anyelement) RETURNS anyelement AS 
      $$ 
      BEGIN 
        RETURN COALESCE(v,s); 
      END; 
      $$ LANGUAGE PLPGSQL IMMUTABLE; 
      
      CREATE AGGREGATE GapFill(anyelement) ( 
        SFUNC=GapFillInternal, 
        STYPE=anyelement 
      ); 
  `);

  // token price registry to avoid manipulating and indexing strings on the other tables
  await db_query(`
    CREATE TABLE IF NOT EXISTS price_feed (
      price_feed_id serial PRIMARY KEY,

      -- unique price feed identifier
      feed_key varchar NOT NULL UNIQUE,

      -- what this price represents, can be used to get an usd value or a token conversion rate
      from_asset_key character varying NOT NULL,
      to_asset_key character varying NOT NULL,

      -- all relevant price feed data: eol, external_id, etc
      price_feed_data jsonb NOT NULL
    );
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS product (
      product_id serial PRIMARY KEY,

      -- the product unique external key
      product_key varchar UNIQUE NOT NULL,

      chain chain_enum NOT NULL,
      
      -- the successive prices to apply to balance to get to usd
      price_feed_1_id integer null references price_feed(price_feed_id),
      price_feed_2_id integer null references price_feed(price_feed_id),
      
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
      balance_diff evm_decimal_256 not null, -- with decimals applied, used to compute P&L

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

  // helper table to get an easy access to a list of blocks + timestamps
  await db_query(`
    CREATE TABLE IF NOT EXISTS block_ts (
      datetime timestamptz not null,
      chain chain_enum not null,
      block_number integer not null,
      block_data jsonb not null
    );
    CREATE UNIQUE INDEX IF NOT EXISTS block_ts_uniq ON block_ts(chain, block_number, datetime);

    SELECT create_hypertable(
      relation => 'block_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '7 days',
      if_not_exists => true
    );
  `);

  // helper timeseries functions, mostly because we need them for grafana
  await db_query(`
    CREATE OR REPLACE FUNCTION narrow_gapfilled_investor_balance(
        _time_from timestamptz, _time_to timestamptz, _interval interval,
        _investor_id integer, _product_ids integer[]
    ) returns table (datetime timestamptz, product_id integer, balance numeric, balance_diff numeric) AS 
    $$
        -- find out investor's investments inside the requested range
        with maybe_empty_investments_ts as (
            SELECT
                time_bucket_gapfill(_interval, b.datetime) as datetime,
                b.product_id,
                sum(balance_diff) as balance_diff,
                locf(last(b.balance::numeric, b.datetime)) as balance
            from investment_balance_ts b
            WHERE
                b.datetime BETWEEN _time_from AND _time_to
                and b.investor_id = _investor_id
                and b.product_id in (select unnest(_product_ids))
            GROUP BY 1, 2
        ),
        -- generate a full range of timestamps for these products and merge with actual investments
        -- this step is neccecary to fill gaps with nulls when there is no investments
        investments_ts as (
            select 
                ts.datetime,
                p.product_id,
                i.balance,
                i.balance_diff
            from generate_series(time_bucket(_interval, _time_from), time_bucket(_interval, _time_to), _interval) ts(datetime)
                cross join (select UNNEST(_product_ids)) as p(product_id)
                left join maybe_empty_investments_ts i on ts.datetime = i.datetime and i.product_id = p.product_id
        ),
        -- go fetch the invetor balance before the requested range and merge it with the actual investments
        balance_with_gaps_ts as (
            select
                time_bucket(_interval, _time_from - _interval) as datetime,
                b.product_id,
                last(b.balance::numeric, b.datetime) as balance,
                sum(b.balance_diff) as balance_diff
            from investment_balance_ts b
                where b.datetime < _time_from
                and b.investor_id = _investor_id
                and b.product_id in (select unnest(_product_ids))
            group by 1,2

            union all

            select * from investments_ts
        ),
        -- propagate the data (basically does locf's job but on the whole range)
        balance_gf_ts as (
            select 
                b.datetime,
                b.product_id,
                gapfill(b.balance) over (partition by b.product_id order by b.datetime) as balance,
                b.balance_diff
            from balance_with_gaps_ts b
        )
        select *
        from balance_gf_ts
    $$
    language sql;

    CREATE OR REPLACE FUNCTION narrow_gapfilled_price(
      _time_from timestamptz, _time_to timestamptz, _interval interval,
      _price_feed_ids integer[]
    ) returns table (datetime timestamptz, price_feed_id integer, price numeric) AS 
    $$
      -- find out the price inside the requested range
      with maybe_empty_price_ts as (
          SELECT
              time_bucket_gapfill(_interval, p.datetime) as datetime,
              p.price_feed_id,
              locf(last(p.price::numeric, p.datetime)) as price
          from price_ts p
          WHERE
              p.datetime BETWEEN _time_from AND _time_to
              and p.price_feed_id in (select unnest(_price_feed_ids))
          GROUP BY 1, 2
      ),
      -- generate a full range of timestamps for these price feeds and merge with prices
      -- this step is neccecary to fill gaps with nulls when there is no price available at all
      price_full_ts as (
          select 
              ts.datetime,
              pf.price_feed_id,
              p.price
          from generate_series(time_bucket(_interval, _time_from), time_bucket(_interval, _time_to), _interval) ts(datetime)
              cross join (select UNNEST(_price_feed_ids)) as pf(price_feed_id)
              left join maybe_empty_price_ts p on ts.datetime = p.datetime and p.price_feed_id = pf.price_feed_id
      ),
      -- go fetch the prices before the requested range and merge it with the actual prices
      price_with_gaps_ts as (
          select
              time_bucket(_interval, p.datetime) as datetime,
              p.price_feed_id,
              last(p.price::numeric, p.datetime) as price
          from price_ts p
              where p.datetime < _time_from
              and p.price_feed_id in (select unnest(_price_feed_ids))
          group by 1,2

          union all

          select * from price_full_ts
      ),
      -- propagate the data (basically does locf's job but on the whole range)
      price_gf_ts as (
          select 
              p.datetime,
              p.price_feed_id,
              gapfill(p.price) over (partition by p.price_feed_id order by p.datetime) as price
          from price_with_gaps_ts p
      )
      select *
      from price_gf_ts
    $$
    language sql;
  `);

  logger.info({ msg: "Migrate done" });
}
