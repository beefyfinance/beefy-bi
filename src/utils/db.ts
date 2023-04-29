import { Client as PgClient, ClientConfig as PgClientConfig } from "pg";
import * as pgcs from "pg-connection-string";
import pgf from "pg-format";
import { allChainIds } from "../types/chain";
import { ConnectionTimeoutError, isConnectionTimeoutError, withTimeout } from "./async";
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

/**
 * How to store rewards:
 *
 * Whatever the solution there should be
 * - a snapshot at multiple point in time (cannot use the ppfs trick since rewards are unpredictable)
 * - an import status per user of the vault per boost/gov
 * - tracking of pending rewards and individual reward claims
 *
 * 1. have an investment_balance master table with all relevant data
 * - pro: everything is in one place
 * - pro: not much to change
 * - con: have to design for all possible scenarios (no rewards, multiple rewards, etc)
 * - con: have to accommodate both the deposit/withdraws in and out the boost and the many reward snapshots in the same table
 * - con: would need to complexify the import_state so we can import both withdraw and rewards in the same timeseries
 *        or have multiple import states for the same time series, one for the boost deposits/withdraws and one for the rewards
 * => ✅ quicker to implement, simpler data model, not that bad of a solution anyway
 *
 * 2. have a separate reward_snapshot table
 * - pro: no change to existing code, just add a new table
 * - pro: easy to reason about
 * - pro: easy to merge into another data structure later on
 * - con: need to join the two tables to get the current balance
 * - con: the reward table will look just like the investment_balance_ts table
 * => ❌ too much separation of concerns
 *
 * 3. have multiple balances for each product, extract product_id and investor_id into a balance_feed table so that we can have multiple balances per investor per product
 * - pro: very flexible
 * - con: heavy change to existing code to account for a small number of cases
 * - con: balance data is spread across multiple timeseries
 * => ❌ too much complexity for too little gain
 *
 */

export type DbClient = {
  connect: PgClient["connect"];
  query: PgClient["query"];
  end: PgClient["end"];
  on: PgClient["on"];
};

let sharedClient: DbClient | null = null;
const appNameCounters: Record<string, number> = {};
export async function getDbClient({ appName = "beefy", freshClient = false }: { appName?: string; freshClient?: boolean }) {
  if (!appNameCounters[appName]) {
    appNameCounters[appName] = 0;
  }

  if (sharedClient === null) {
    appNameCounters[appName] += 1;
    const appNameToUse = appName + ":common:" + appNameCounters[appName];

    const pgUrl = TIMESCALEDB_URL;
    const config = pgcs.parse(pgUrl) as any as PgClientConfig;
    logger.trace({ msg: "Instantiating new shared pg client", data: { appNameToUse } });
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
    logger.trace({ msg: "Instantiating new unique pg client", data: { appNameToUse } });
    return new PgClient({ ...config, application_name: appNameToUse });
  }

  return sharedClient;
}

// inject pg client as first argument
export function withDbClient<TArgs extends any[], TRes>(
  fn: (client: DbClient, ...args: TArgs) => Promise<TRes>,
  { appName, connectTimeoutMs = 10_000, logInfos }: { appName: string; connectTimeoutMs?: number; logInfos: LogInfos },
): (...args: TArgs) => Promise<TRes> {
  return async (...args: TArgs) => {
    const pgClient = await getDbClient({ appName, freshClient: false });
    let res: TRes;
    try {
      logger.trace({ msg: "Connecting to pg", data: { appName, connectTimeoutMs } });
      await withTimeout(() => pgClient.connect(), connectTimeoutMs, logInfos);
      res = await fn(pgClient, ...args);
    } finally {
      await pgClient.end();
    }
    return res;
  };
}

export async function db_transaction<TRes>(
  fn: (client: DbClient) => Promise<TRes>,
  {
    appName,
    connectTimeoutMs = 10_000,
    queryTimeoutMs = 10_000,
    logInfos,
  }: { appName: string; connectTimeoutMs?: number; queryTimeoutMs?: number; logInfos: LogInfos },
) {
  const pgClient = await getDbClient({ appName, freshClient: true });
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

export async function db_query<RowType>(sql: string, params: any[] = [], client: DbClient | null = null): Promise<RowType[]> {
  logger.trace({ msg: "Executing query", data: { sql, params } });
  let useClient: DbClient | null = client;
  if (useClient === null) {
    const pool = await getDbClient({ freshClient: false });
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
    // if the query ended because of a connection timeout, we wrap it in a custom error
    // so that we can handle it upper in the stack and retry the query/transaction
    if (isConnectionTimeoutError(error)) {
      throw new ConnectionTimeoutError("Query timeout", error);
    }
    logger.error({ msg: "Query error", data: { sql, params, error } });
    throw error;
  }
}

export async function db_query_one<RowType>(sql: string, params: any[] = [], client: DbClient | null = null): Promise<RowType | null> {
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

async function hasPolicy(
  tableSchema: string,
  tableName: string,
  tableType: "hypertable" | "continuous_aggregate",
  policyType: "policy_retention" | "policy_refresh_continuous_aggregate" | "policy_compression",
) {
  let res: any = null;
  if (tableType === "continuous_aggregate") {
    res = await db_query_one(
      `
        select p.*
        from timescaledb_information.continuous_aggregates a
        left join timescaledb_information.jobs p on a.materialization_hypertable_schema = p.hypertable_schema and a.materialization_hypertable_name = p.hypertable_name
        where a.view_schema = %L and a.view_name = %L
        and p.proc_name = %L
      `,
      [tableSchema, tableName, policyType],
    );
  } else if (tableType === "hypertable") {
    res = await db_query_one(
      `
        select p.*
        from timescaledb_information.hypertables h
        left join timescaledb_information.jobs p on h.hypertable_schema = p.hypertable_schema and h.hypertable_name = p.hypertable_name
        where h.hypertable_schema = %L and h.hypertable_name = %L
        and p.proc_name = %L
      `,
      [tableSchema, tableName, policyType],
    );
  }
  return res !== null;
}

// postgresql don't have "create type/domain if not exists"
async function typeExists(typeName: string) {
  const res = await db_query_one(`SELECT * FROM pg_type WHERE typname = %L`, [typeName]);
  return res !== null;
}

async function timescaledbJobExists(procName: string) {
  const res = await db_query_one(`SELECT * FROM timescaledb_information.jobs WHERE proc_name = %L`, [procName]);
  return res !== null;
}

export async function db_migrate() {
  logger.info({ msg: "Migrate begin" });

  // extensions
  await db_query(`
    create extension if not exists "uuid-ossp";
  `);

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

  if (!(await typeExists("evm_decimal_256_nullable"))) {
    await db_query(`
      CREATE DOMAIN evm_decimal_256_nullable 
        -- 24 is the max decimals in current addressbook, might change in the future
        -- 100 is the maximum number of digits stored, not the reserved space
        AS NUMERIC(100, 24)
        CHECK (VALUE is NULL OR nullif(VALUE, 'NaN') is not null);
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
      
      CREATE OR REPLACE AGGREGATE GapFill(anyelement) ( 
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

      -- the reward price feed to apply to the reward balance to get to usd
      pending_rewards_price_feed_id integer null references price_feed(price_feed_id),
      
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

  // list of ignored addresses
  await db_query(`
    CREATE TABLE IF NOT EXISTS ignore_address (
      chain chain_enum NOT NULL,
      address evm_address_bytea NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ignore_address_chain_address_idx ON ignore_address (chain, address);
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS investment_balance_ts (
      datetime timestamptz not null,

      -- some chains have the same timestamp for distinct block numbers so we need to save the block number to the distinct key
      block_number integer not null,

      -- whatever contract the user is invested with
      product_id integer not null references product(product_id),
      investor_id integer not null references investor(investor_id),

      -- the investment details, all numbers have decimal applied
      transaction_hash evm_trx_hash,
      balance evm_decimal_256 not null, -- balance of the investment after the block was applied
      balance_diff evm_decimal_256 not null, -- how much the investment changed at this block
      pending_rewards evm_decimal_256_nullable null, -- how much rewards are pending to be claimed
      pending_rewards_diff evm_decimal_256_nullable null -- how much rewards changed at this block
    );
    CREATE UNIQUE INDEX IF NOT EXISTS investment_balance_ts_uniq ON investment_balance_ts(product_id, investor_id, block_number, datetime);

    SELECT create_hypertable(
      relation => 'investment_balance_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '100 days',
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

      price evm_decimal_256 not null
    );

    -- make sure we don't create a unique index on a null value because all nulls are considered different
    CREATE UNIQUE INDEX IF NOT EXISTS price_ts_wi_chain_uniq ON price_ts(price_feed_id, block_number, datetime);
    CREATE INDEX IF NOT EXISTS price_ts_15min_idx ON price_ts (price_feed_id, time_bucket('15min', datetime));

    SELECT create_hypertable(
      relation => 'price_ts',
      time_column_name => 'datetime', 
      chunk_time_interval => INTERVAL '100 days', 
      if_not_exists => true
    );
  `);

  // pre-aggregated price data
  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS price_ts_cagg_1h
    WITH (timescaledb.continuous) AS
    SELECT price_feed_id,
      time_bucket(INTERVAL '1 hour', datetime) AS datetime,
      AVG(price) as price_avg,
      MAX(price) as price_high,
      MIN(price) as price_low,
      FIRST(price, datetime) as price_open,
      LAST(price, datetime) as price_close
    FROM price_ts
    GROUP BY 1,2
    WITH NO DATA;

    /*
     * https://docs.timescale.com/timescaledb/latest/how-to-guides/continuous-aggregates/create-index/
     * When you create a continuous aggregate, an index is automatically created for each GROUP BY column. 
     * The index is a composite index, combining the GROUP BY column with the time_bucket column.
     */
  `);

  if (!(await hasPolicy("public", "price_ts_cagg_1h", "continuous_aggregate", "policy_refresh_continuous_aggregate"))) {
    await db_query(`
      SELECT add_continuous_aggregate_policy('price_ts_cagg_1h',
        start_offset => INTERVAL '1 day',
        end_offset => INTERVAL '2 hours',
        schedule_interval => INTERVAL '4 hour'
      );
    `);
  }
  if (!(await hasPolicy("public", "price_ts_cagg_1h", "continuous_aggregate", "policy_retention"))) {
    await db_query(`
      SELECT add_retention_policy('price_ts_cagg_1h', INTERVAL '6 months');
    `);
  }

  await db_query(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS price_ts_cagg_1d
    WITH (timescaledb.continuous) AS
    SELECT price_feed_id,
      time_bucket(INTERVAL '1 day', datetime) AS datetime,
      AVG(price) as price_avg,
      MAX(price) as price_high,
      MIN(price) as price_low,
      FIRST(price, datetime) as price_open,
      LAST(price, datetime) as price_close
    FROM price_ts
    GROUP BY 1,2
    WITH NO DATA;

    /*
     * https://docs.timescale.com/timescaledb/latest/how-to-guides/continuous-aggregates/create-index/
     * When you create a continuous aggregate, an index is automatically created for each GROUP BY column. 
     * The index is a composite index, combining the GROUP BY column with the time_bucket column.
     */
  `);

  if (!(await hasPolicy("public", "price_ts_cagg_1d", "continuous_aggregate", "policy_refresh_continuous_aggregate"))) {
    await db_query(`
      SELECT add_continuous_aggregate_policy('price_ts_cagg_1d',
        start_offset => INTERVAL '3 day',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '12 hour'
      );
    `);
  }

  // a table to store which data we already imported and which range needs to be retried
  // we need this because if there is no data on some date range, maybe we already fetched it and there is no data
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
      block_number integer not null
    );
    CREATE UNIQUE INDEX IF NOT EXISTS block_ts_uniq ON block_ts(chain, block_number, datetime);

    SELECT create_hypertable(
      relation => 'block_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '100 days',
      if_not_exists => true
    );
  `);

  // create a denormalized table specifically built to serve beefy's investor page
  // this table is partitioned by investor id and is meant to be optimized for retrieving
  // the whole investor history in one go so the client can compute P&L locally.
  // Most fields are denormalized to avoid joins and nullable to account for data arriving at different times
  // it's only a timescaledb hypertable so we don't have to manage partitions manually, nothing else
  await db_query(`
    CREATE TABLE IF NOT EXISTS beefy_investor_timeline_cache_ts (
      investor_id integer not null references investor(investor_id),
      product_id integer not null references product(product_id),
      datetime timestamptz not null,
      block_number integer not null,

      -- store everything as double precision as we can afford to lose some precision for speed
      transaction_hash evm_trx_hash,
      balance double precision,
      balance_diff double precision,
      share_to_underlying_price double precision,
      underlying_balance double precision, -- balance * share_to_underlying_price
      underlying_diff double precision, -- balance_diff * share_to_underlying_price
      underlying_to_usd_price double precision,
      usd_balance double precision, -- underlying_balance * underlying_to_usd_price
      usd_diff double precision, -- underlying_diff * underlying_to_usd_price
      pending_rewards double precision,
      pending_rewards_diff double precision,
      pending_rewards_to_usd_price double precision,
      pending_rewards_usd_balance double precision, -- pending_rewards * pending_rewards_to_usd_price
      pending_rewards_usd_diff double precision -- pending_rewards_diff * pending_rewards_to_usd_price
    );

    SELECT create_hypertable(
      relation => 'beefy_investor_timeline_cache_ts', 
      time_column_name => 'datetime',
      if_not_exists => true,
      -- we want all data in one chunk
      chunk_time_interval => INTERVAL '1000 years',
      partitioning_column => 'investor_id',
      number_partitions => 200
    );

    CREATE UNIQUE INDEX IF NOT EXISTS beefy_investor_cache_uniq_idx ON beefy_investor_timeline_cache_ts (investor_id, product_id, block_number, datetime);

    -- query index
    CREATE INDEX IF NOT EXISTS beefy_investor_cache_query_idx ON beefy_investor_timeline_cache_ts (investor_id);

    -- find empty price index
    CREATE INDEX IF NOT EXISTS beefy_investor_cache_empty_price_idx ON beefy_investor_timeline_cache_ts (time_bucket('15min', datetime)) where underlying_to_usd_price is null;
  `);

  // track down rpc errors to be able to understand what's going on without looking at logs
  await db_query(`
    CREATE TABLE IF NOT EXISTS rpc_error_ts (
      chain chain_enum NOT NULL,
      datetime timestamptz NOT NULL,
      rpc_url character varying NOT NULL,

      -- use json instead of jsonb to keep the error and query as close to the original as possible
      -- this is because jsonb will reorder keys and remove whitespaces
      request json NOT NULL,
      response json NOT NULL
    );

    CREATE INDEX IF NOT EXISTS rpc_error_chain_idx ON rpc_error_ts (chain, datetime);

    SELECT create_hypertable(
      relation => 'rpc_error_ts', 
      time_column_name => 'datetime',
      if_not_exists => true,
      chunk_time_interval => INTERVAL '1 day'
    );
  `);

  if (!(await hasPolicy("public", "rpc_error_ts", "hypertable", "policy_retention"))) {
    await db_query(`
      SELECT add_retention_policy('rpc_error_ts', INTERVAL '1 month');
    `);
  }

  // create a job to snapshot import metrics every 15 minutes
  if (!(await timescaledbJobExists("snapshot_import_metrics"))) {
    await db_query(`
      create table if not exists import_state_metrics_ts (
        datetime timestamptz not null,
        chain chain_enum not null,
        import_type text not null,
        eol boolean not null,
        errors_count bigint not null,
        success_count bigint not null,
        not_covered_yet bigint not null
      );
      
      SELECT create_hypertable(
        relation => 'import_state_metrics_ts', 
        time_column_name => 'datetime',
        chunk_time_interval => INTERVAL '30 days',
        if_not_exists => true
      );
      
      create index if not exists import_state_metrics_ts_chain_import_type_idx on import_state_metrics_ts (chain, import_type, datetime);
      
      
      create or replace PROCEDURE snapshot_import_metrics(job_id INT, config JSONB) 
      as $$
      insert into import_state_metrics_ts (
        datetime,
        chain,
        import_type,
        eol,
        errors_count,
        success_count,
        not_covered_yet
      )
          with max_block_number_by_chain as materialized (
            select 
              (i.import_data->>'chain')::chain_enum as chain,
              max((import_data->>'chainLatestBlockNumber')::integer) as last_covered
            from import_state i
            where i.import_data->>'type' = 'product:investment'
            group by 1
          ),
          procuct_block_stats as (
            select 
              p.chain,
              p.product_id,
              i.import_data->>'type' as import_type,
              (p.product_data->>'dashboardEol') = 'true' as eol,
              b.last_covered - (import_data->>'contractCreatedAtBlock')::integer + 1 as total_blocks_to_cover,
              jsonb_int_ranges_size_sum(import_data->'ranges'->'coveredRanges') as blocks_covered,
              jsonb_int_ranges_size_sum(import_data->'ranges'->'toRetry') as blocks_to_retry
            from product p
            left join import_state i on p.product_id = (i.import_data->>'productId')::integer
            left join max_block_number_by_chain b on p.chain = b.chain
          )
          select 
            now(),
            chain,
            import_type,
            eol,
            sum(blocks_to_retry) as errors_count,
            sum(blocks_covered) - sum(blocks_to_retry) as success_count,
            sum(total_blocks_to_cover) - sum(blocks_covered) as not_covered_yet
          from procuct_block_stats
          where import_type is not null
          group by 1, 2, 3, 4
      $$
        LANGUAGE SQL;
      
      SELECT add_job('snapshot_import_metrics', '1min', config => null);
    `);
  }

  await db_query(`
    create table if not exists rpc (
      rpc_id serial PRIMARY KEY,

      -- the chain this rpc is for
      chain chain_enum not null,

      -- the rpc unique external key
      rpc_key varchar UNIQUE NOT NULL,

      -- rpc url where secrets are template variables
      rpc_template_url varchar not null
    );
  `);

  if (!(await typeExists("rpc_call_reason_enum"))) {
    await db_query(`
        CREATE TYPE rpc_call_reason_enum AS ENUM ();
    `);
  }
  for (const chain of allChainIds) {
    await db_query(`ALTER TYPE chain_enum ADD VALUE IF NOT EXISTS %L`, [chain]);
  }

  await db_query(`
    create table if not exists rpc_call_ts (
      datetime timestamptz not null, -- when was this call initiated
      rpc_id integer not null references rpc(rpc_id),

      -- performance fields
      waiting_for_lock_ms integer not null,
      call_duration_ms integer not null,
      request_bytes integer not null,
      response_bytes integer not null,

      -- why was this call made
      reason varchar not null, -- include mode (historical, recent), importer (ppfs snapshot, investments, etc)

      -- what was inside this call

      block_number integer, -- the block number targeted by this call, null if not relevant
      json_rpc_batch_size smallint not null, -- set to zero if not a batch call
      internal_batch_size smallint not null, -- multicall3 count or getLogs address batch
      internal_result_count smallint not null, -- getLogs event count

      eth_getlogs_avg_size smallint not null,
      eth_getlogs_count smallint not null,
      eth_call_count smallint not null, 
      eth_blocknumber_count smallint not null,
      eth_getblock_count smallint not null,
      eth_gettransactionreceipt_count smallint not null,

      -- result
      got_lock boolean,
      got_response boolean,
      success_response boolean,
      all_success_in_batch boolean,

      -- other interesting data?
      rpc_call_data jsonb not null
    );
    
    SELECT create_hypertable(
      relation => 'rpc_call_ts', 
      time_column_name => 'datetime',
      chunk_time_interval => INTERVAL '1 days',
      if_not_exists => true
    );
  `);

  logger.info({ msg: "Migrate done" });
}
