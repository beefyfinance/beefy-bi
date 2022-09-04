import { Pool, PoolConfig, PoolClient } from "pg";
import pgf from "pg-format";
import * as pgcs from "pg-connection-string";
import { TIMESCALEDB_URL } from "./config";
import { normalizeAddress } from "./ethers";
import { allChainIds, Chain } from "../types/chain";
import { rootLogger } from "./logger2";
import { Dictionary, keyBy, uniqBy, zipWith } from "lodash";
import * as Rx from "rxjs";
import { batchQueryGroup } from "./rxjs/utils/batch-query-group";

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

export async function db_query<RowType>(
  sql: string,
  params: any[] = [],
  client: PoolClient | null = null,
): Promise<RowType[]> {
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

export async function db_query_one<RowType>(
  sql: string,
  params: any[] = [],
  client: PoolClient | null = null,
): Promise<RowType | null> {
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

// postgresql don't have "create type/domain if not exists"
async function typeExists(typeName: string) {
  const res = await db_query_one(`SELECT * FROM pg_type WHERE typname = %L`, [typeName]);
  return res !== null;
}

async function migrate() {
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
  `);

  // an address and transaction table to avoid bloating the tables and indices
  await db_query(`
    CREATE TABLE IF NOT EXISTS evm_address (
      evm_address_id serial PRIMARY KEY,
      chain chain_enum NOT NULL,
      address evm_address_bytea NOT NULL,
      metadata jsonb NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS evm_address_uniq ON evm_address(chain, address);
  `);

  await db_query(`
    CREATE TABLE IF NOT EXISTS evm_transaction (
      evm_transaction_id serial PRIMARY KEY,
      chain chain_enum NOT NULL,
      hash evm_trx_hash NOT NULL,
      block_number integer not null,
      block_datetime TIMESTAMPTZ NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS evm_transaction_uniq ON evm_transaction(chain, hash);
  `);

  // stores vault transfers of shares
  // may represent a virtual vault (that doesn't give a token back)
  // in this case we have a 1:1 mapping between the underlying and the share
  await db_query(`
    CREATE TABLE IF NOT EXISTS vault_shares_transfer_ts (
      datetime timestamptz not null,

      -- make it easier to debug and/or partition later
      block_number integer not null,
      chain chain_enum not null,

      evm_transaction_id integer not null references evm_transaction(evm_transaction_id),
      owner_evm_address_id integer not null references evm_address(evm_address_id),
      vault_evm_address_id integer not null references evm_address(evm_address_id),

      -- all numeric fields have decimals applied
      shares_balance_diff evm_decimal_256 not null,
      shares_balance_after evm_decimal_256 null -- can be null if we can't query the archive node
    );
    CREATE UNIQUE INDEX IF NOT EXISTS vault_shares_transfer_ts_uniq ON vault_shares_transfer_ts(owner_evm_address_id, vault_evm_address_id, evm_transaction_id, datetime);

    SELECT create_hypertable(
      relation => 'vault_shares_transfer_ts', 
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

      -- make it easier to debug and/or partition later
      block_number integer not null,
      chain chain_enum not null,

      vault_evm_address_id integer not null references evm_address(evm_address_id),

      -- all numeric fields have decimals applied
      shares_to_underlying_rate evm_decimal_256 not null
    );
    CREATE UNIQUE INDEX IF NOT EXISTS vault_to_underlying_rate_ts_uniq ON vault_to_underlying_rate_ts(vault_evm_address_id, datetime);

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
    CREATE UNIQUE INDEX IF NOT EXISTS asset_price_ts_uniq ON asset_price_ts(price_feed_key, datetime);
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
      chain chain_enum NOT NULL,
      contract_evm_address_id integer not null references evm_address(evm_address_id),
      underlying_evm_address_id integer not null references evm_address(evm_address_id),
      end_of_life boolean not null,
      has_erc20_shares_token boolean not null,
      assets_price_feed_keys varchar[] not null
    );

    CREATE UNIQUE INDEX IF NOT EXISTS beefy_vault_uniq ON beefy_vault(contract_evm_address_id);
  `);

  // a table to store which vault we already imported and which range needs to be retried
  await db_query(`
    CREATE TABLE IF NOT EXISTS import_status (
      import_key varchar NOT NULL,

      -- either blocknumber range or timestamp range
      imported_range int4range NOT NULL,
      
      -- those are ranges with errors that need to be retried
      ranges_to_retry int4range[] NOT NULL
    );
  `);

  logger.info({ msg: "Migrate done" });

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

interface DbEvmAddress {
  evm_address_id: number;
  chain: Chain;
  address: string;
  metadata: {
    erc20?: {
      name: string | null;
      decimals: number;
      price_feed_key: string;
    };
  };
}

// upsert the address of all objects and return the id in the specified field
export function mapAddressToEvmAddressId<
  TObj,
  TKey extends string,
  TParams extends Omit<DbEvmAddress, "evm_address_id">,
>(
  client: PoolClient,
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: number })[]> {
  const getKey = (param: TObj) => {
    const { chain, address } = getParams(param);
    return `${chain}-${address.toLocaleLowerCase()}`;
  };
  const process = async (params: TParams[]) => {
    type TRes = { evm_address_id: number; chain: Chain; address: string };
    const results = await db_query<TRes>(
      `INSERT INTO evm_address (chain, address, metadata) VALUES %L
        ON CONFLICT (chain, address) DO UPDATE SET metadata = jsonb_merge(evm_address.metadata, EXCLUDED.metadata)
        RETURNING evm_address_id, chain, bytea_to_hexstr(address)`,
      [params.map(({ chain, address, metadata }) => [chain, strAddressToPgBytea(address), metadata])],
      client,
    );
    // ensure results are in the same order as the params
    const addressIdMap = keyBy(results, getKey) as Dictionary<TRes>;
    return params.map(({ chain, address }) => {
      const key = `${chain}-${address.toLocaleLowerCase()}`;
      return addressIdMap[key].evm_address_id;
    });
  };

  return batchQueryGroup(getParams, getKey, process, toKey);
}

export function mapEvmAddressIdToAddress<TObj, TKey extends string>(
  client: PoolClient,
  getAddrId: (obj: TObj) => number,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: DbEvmAddress })[]> {
  const process = async (ids: number[]) => {
    const results = await db_query<DbEvmAddress>(
      `SELECT evm_address_id, chain, bytea_to_hexstr(address) as address, metadata FROM evm_address WHERE evm_address_id IN (%L)`,
      [ids],
      client,
    );
    // ensure results are in the same order as the params
    const addressIdMap = keyBy(results, (r) => r.evm_address_id) as Dictionary<DbEvmAddress>;
    return ids.map((id) => addressIdMap[id]);
  };

  return batchQueryGroup(getAddrId, getAddrId, process, toKey);
}

interface DbEvmTransaction {
  evm_transaction_id: number;
  chain: Chain;
  hash: string;
  block_number: number;
  block_datetime: Date;
}

// upsert the address of all objects and return the id in the specified field
export function mapTransactionToEvmTransactionId<
  TObj,
  TKey extends string,
  TParams extends Omit<DbEvmTransaction, "evm_transaction_id">,
>(
  client: PoolClient,
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: number })[]> {
  const getKey = (param: TObj) => {
    const { chain, hash } = getParams(param);
    return `${chain}-${hash.toLocaleLowerCase()}`;
  };

  const process = async (params: TParams[]) => {
    type TRes = { evm_transaction_id: number; chain: Chain; hash: string };
    const results = await db_query<TRes>(
      `INSERT INTO evm_transaction (chain, hash, block_number, block_datetime) VALUES %L
        ON CONFLICT (chain, hash) 
        -- DO NOTHING -- can't use DO NOTHING because we need to return the id
        DO UPDATE SET block_number = EXCLUDED.block_number, block_datetime = EXCLUDED.block_datetime
        RETURNING evm_transaction_id, chain, bytea_to_hexstr(hash)`,
      [
        params.map(({ chain, hash, block_number, block_datetime }) => [
          chain,
          strAddressToPgBytea(hash),
          block_number,
          block_datetime.toISOString(),
        ]),
      ],
      client,
    );
    // ensure results are in the same order as the params
    const addressIdMap = keyBy(results, getKey) as Dictionary<TRes>;
    return params.map(({ chain, hash }) => {
      const key = `${chain}-${hash.toLocaleLowerCase()}`;
      return addressIdMap[key].evm_transaction_id;
    });
  };

  return batchQueryGroup(getParams, getKey, process, toKey);
}

interface DbBeefyVault {
  vault_id: number;
  chain: Chain;
  vault_key: string;
  contract_evm_address_id: number;
  underlying_evm_address_id: number;
  end_of_life: boolean;
  has_erc20_shares_token: boolean;
  assets_price_feed_keys: string[];
}

export function vaultListUpdates$(client: PoolClient, pollFrequencyMs: number = 1000 * 60 * 60) {
  logger.info({ msg: "Listening to vault updates" });
  // refresh vault list every hour
  return Rx.interval(pollFrequencyMs).pipe(
    // start immediately, otherwise we have to wait for the interval to start
    Rx.startWith(0),

    Rx.tap(() => logger.debug({ msg: "vaultListUpdatesObservable: fetching vault updates" })),

    // fetch the vault list
    Rx.switchMap(() =>
      db_query<DbBeefyVault>(
        `SELECT vault_id, chain, vault_key, contract_evm_address_id, underlying_evm_address_id, end_of_life, has_erc20_shares_token, assets_price_feed_keys FROM beefy_vault`,
      ),
    ),

    // add the vault addresses while we are doing batch work
    mapEvmAddressIdToAddress(client, (v) => v.contract_evm_address_id, "contract_evm_address"),
    mapEvmAddressIdToAddress(client, (v) => v.underlying_evm_address_id, "underlying_evm_address"),

    Rx.tap(() => logger.debug({ msg: "vaultListUpdatesObservable: emitting vault update" })),

    // flatten vault list into a stream of vaults
    Rx.mergeMap((vaults) => Rx.from(vaults)),

    // only emit if eol changed, only one time per vault
    Rx.distinct((vault) => `${vault.vault_id}-${vault.end_of_life ? "eol" : "active"}`),
  );
}

export function vaultList$(client: PoolClient) {
  logger.debug({ msg: "Fetching vaults from db" });
  return Rx.of(
    db_query<DbBeefyVault>(
      `SELECT vault_id, chain, vault_key, contract_evm_address_id, underlying_evm_address_id, end_of_life, has_erc20_shares_token, assets_price_feed_keys FROM beefy_vault`,
    ).then((vaults) => (vaults.length > 0 ? vaults : Promise.reject("No vaults found"))),
  ).pipe(
    Rx.mergeAll(),

    // add the vault addresses while we are doing batch work
    mapEvmAddressIdToAddress(client, (v) => v.contract_evm_address_id, "contract_evm_address"),
    mapEvmAddressIdToAddress(client, (v) => v.underlying_evm_address_id, "underlying_evm_address"),

    Rx.tap(() => logger.debug({ msg: "vaultListUpdatesObservable: emitting vault update" })),

    // flatten vault list into a stream of vaults
    Rx.mergeMap((vaults) => Rx.from(vaults)),
  );
}
