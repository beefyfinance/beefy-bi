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
      CREATE SCHEMA IF NOT EXISTS partitions;

      CREATE TABLE IF NOT EXISTS erc20_transfer (
          chain varchar(10) NOT NULL,
          contract_address varchar(42) NOT NULL,
          time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
          block_number INTEGER NOT NULL,
          from_address varchar(42) not null,
          to_address varchar(42) not null,
          value text not null
      ) PARTITION BY LIST (contract_address);

      CREATE TABLE IF NOT EXISTS token_price_ts (
        time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        chain varchar(10) NOT NULL,
        contract_address varchar(42) NOT NULL,
        price double precision not null
      );
      SELECT create_hypertable('token_price_ts', 'time', chunk_time_interval => INTERVAL '30 day', if_not_exists => true);
      SELECT add_dimension('token_price_ts', 'chain', number_partitions => 15, if_not_exists => true);
      SELECT add_dimension('token_price_ts', 'contract_address', number_partitions => 500, if_not_exists => true);

      create index if not exists idx_token_price_contract_address on token_price_ts(chain, contract_address);

      
      CREATE TABLE IF NOT EXISTS vault_token_to_underlying_rate (
        chain varchar(10) NOT NULL,
        contract_address varchar(42) NOT NULL,
        time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        block_number INTEGER NOT NULL,
        rate text not null
    ) PARTITION BY LIST (contract_address);


      
    CREATE TABLE IF NOT EXISTS vault_strategy (
        chain varchar(10) NOT NULL,
        contract_address varchar(42) NOT NULL,
        time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        block_number INTEGER NOT NULL,
        strategy_address varchar(42) NOT NULL
    );
    CREATE unique index if not exists idx_vault_strategy_fk on vault_strategy(chain, contract_address, strategy_address);
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

export async function prepareInsertVaultRateBatch(
  _: Chain,
  contractAddress: string
) {
  return db_query(
    `
    CREATE TABLE IF NOT EXISTS partitions."vault_token_to_underlying_rate_%s" PARTITION OF vault_token_to_underlying_rate
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

export async function* streamLocalERC20Transfer(
  chain: Chain,
  contractAddress: string
) {
  const res = await db_query<{
    time: Date;
    from_address: string;
    to_address: string;
    value: string;
  }>(
    `
    SELECT 
      time at time zone 'utc' as time,
      from_address,
      to_address,
      value 
    FROM erc20_transfer
    WHERE chain = %L AND contract_address = %L
    order by time asc
  `,
    [chain, contractAddress]
  );

  for (const row of res) {
    const data: { time: Date; from: string; to: string; value: BigInt } = {
      from: row.from_address,
      to: row.to_address,
      value: BigInt(row.value),
      time: new Date(row.time),
    };
    yield data;
  }
}

export async function insertTokenPriceBatch(
  values: {
    chain: string;
    contract_address: string;
    time: string;
    price: number;
  }[]
) {
  logger.verbose(`[DB] Inserting ${values.length} rows into token_price_ts`);
  const valueArr = values.map((val) => [
    val.chain,
    val.contract_address,
    val.time,
    val.price,
  ]);
  return db_query(
    `
    INSERT INTO token_price_ts (
      chain,
      contract_address,
      time,
      price
    ) VALUES %L
  `,
    [valueArr]
  );
}

export async function insertVaultTokenRateBatch(
  values: {
    chain: string;
    contract_address: string;
    time: string;
    block_number: number;
    rate: string;
  }[]
) {
  logger.verbose(
    `[DB] Inserting ${values.length} rows into vault_token_to_underlying_rate`
  );
  const valueArr = values.map((val) => [
    val.chain,
    val.contract_address,
    val.time,
    val.block_number,
    val.rate,
  ]);
  return db_query(
    `
    INSERT INTO vault_token_to_underlying_rate (
      chain,
      contract_address,
      time,
      block_number,
      rate
    ) VALUES %L
    ON CONFLICT DO NOTHING
  `,
    [valueArr]
  );
}

export async function insertVaulStrategyBatch(
  values: {
    chain: string;
    contract_address: string;
    time: string;
    block_number: number;
    strategy_address: string;
  }[]
) {
  logger.verbose(`[DB] Inserting ${values.length} rows into vault_strategy`);
  const valueArr = values.map((val) => [
    val.chain,
    val.contract_address,
    val.time,
    val.block_number,
    val.strategy_address,
  ]);
  return db_query(
    `
    INSERT INTO vault_strategy (
      chain,
      contract_address,
      time,
      block_number,
      strategy_address
    ) VALUES %L
    ON CONFLICT DO NOTHING
  `,
    [valueArr]
  );
}
