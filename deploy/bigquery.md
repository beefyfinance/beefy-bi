# UTILS

## export ddl of a manually created table

```
SELECT
 table_name, ddl
FROM
 `beefy-bi.beefy_data_raw.INFORMATION_SCHEMA.TABLES`
where table_name like '%all_vault%'
```

# Create queries

## external tables

```
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.arbitrum_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/arbitrum/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.aurora_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/aurora/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.avax_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/avax/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.bsc_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/bsc/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.celo_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/celo/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.cronos_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/cronos/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.emerald_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/emerald/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.fantom_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/fantom/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.fuse_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/fuse/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.harmony_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/harmony/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.heco_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/heco/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.metis_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/metis/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.moonbeam_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/moonbeam/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.moonriver_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/moonriver/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.optimism_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/optimism/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.polygon_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/polygon/contracts/*/ERC20/Transfer.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.syscoin_vault_transfer` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, from_address STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/syscoin/contracts/*/ERC20/Transfer.csv"]);

CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.arbitrum_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/arbitrum/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.aurora_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/aurora/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.avax_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/avax/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.bsc_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/bsc/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.celo_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/celo/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.cronos_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/cronos/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.emerald_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/emerald/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.fantom_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/fantom/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.fuse_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/fuse/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.harmony_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/harmony/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.heco_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/heco/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.metis_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/metis/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.moonbeam_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/moonbeam/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.moonriver_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/moonriver/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.optimism_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/optimism/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.polygon_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/polygon/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.syscoin_ppfs_4hour` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, ppfs BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/chain/syscoin/contracts/*/BeefyVaultV6/ppfs_4hour.csv"]);

CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.oracle_price_15min` ( datetime STRING NOT NULL, usd_value BIGNUMERIC NOT NULL ) OPTIONS(format="CSV", uris=["gs://beefy-bi/indexed-data/price/beefy/*/price_15min.csv"]);

CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.arbitrum_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/arbitrum/contracts/*/ERC20_from_self/0x82aF49447D8a07e3bd95BD0d56f35241523fBab1/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.aurora_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/aurora/contracts/*/ERC20_from_self/0xC9BdeEd33CD01541e1eeD10f90519d2C06Fe3feB/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.avax_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/avax/contracts/*/ERC20_from_self/0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.bsc_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/bsc/contracts/*/ERC20_from_self/0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.celo_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/celo/contracts/*/ERC20_from_self/0x471EcE3750Da237f93B8E339c536989b8978a438/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.cronos_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/cronos/contracts/*/ERC20_from_self/0x5C7F8A570d578ED84E63fdFA7b1eE72dEae1AE23/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.emerald_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/emerald/contracts/*/ERC20_from_self/0x21C718C22D52d0F3a789b752D4c2fD5908a8A733/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.fantom_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/fantom/contracts/*/ERC20_from_self/0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.fuse_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/fuse/contracts/*/ERC20_from_self/0x0BE9e53fd7EDaC9F859882AfdDa116645287C629/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.harmony_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/harmony/contracts/*/ERC20_from_self/0xcF664087a5bB0237a0BAd6742852ec6c8d69A27a/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.heco_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/heco/contracts/*/ERC20_from_self/0x5545153CCFcA01fbd7Dd11C0b23ba694D9509A6F/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.metis_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/metis/contracts/*/ERC20_from_self/0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.moonbeam_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/moonbeam/contracts/*/ERC20_from_self/0xAcc15dC74880C9944775448304B263D191c6077F/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.moonriver_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/moonriver/contracts/*/ERC20_from_self/0x98878B06940aE243284CA214f92Bb71a2b032B8A/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.optimism_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/optimism/contracts/*/ERC20_from_self/0x4200000000000000000000000000000000000006/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.polygon_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/polygon/contracts/*/ERC20_from_self/0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270/Transfer.csv"] );
CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.syscoin_native_transfer_from` ( block_number INT64 NOT NULL, block_datetime STRING NOT NULL, to_address STRING NOT NULL, amount BIGNUMERIC NOT NULL ) OPTIONS( format="CSV", uris=["gs://beefy-bi/indexed-data/chain/syscoin/contracts/*/ERC20_from_self/0xd3e822f3ef011Ca5f17D82C956D952D8d7C3A1BB/Transfer.csv"] );




CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.all_vaults`
(
  id STRING NOT NULL,
  token_name STRING NOT NULL,
  token_decimals INTEGER NOT NULL,
  token_address STRING NOT NULL,
  want_address STRING NOT NULL,
  want_decimals INTEGER NOT NULL,
  price_oracle STRUCT<want_oracleId STRING, assets ARRAY<STRING>> NOT NULL
)
OPTIONS(
  format="NEWLINE_DELIMITED_JSON",
  uris=["gs://beefy-bi/indexed-data/chain/*/beefy/vaults.jsonl"]
);

CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.all_vault_data_coverage`
(
  chain STRING,
  vault_id STRING,
  moo_token_address STRING,
  moo_token_decimals INTEGER,
  moo_token_name STRING,
  want_oracle_id STRING,
  want_decimals INTEGER,
  want_address STRING,
  vault_creation_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  vault_creation_block_number INT64,
  vault_creation_transaction FLOAT64,
  first_moo_erc_20_transfer_block_number INT64,
  first_moo_erc_20_transfer_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  last_moo_erc_20_transfer_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  last_moo_erc_20_transfer_block_number INT64,
  wnative_oracle_last_price_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  wnative_oracle_first_price_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  want_oracle_last_price_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  want_oracle_first_price_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  last_ppfs_block_number INT64,
  first_ppfs_block_number INT64,
  first_ppfs_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  last_ppfs_datetime TIMESTAMP OPTIONS(description="bq-datetime"),
  strategies ARRAY<STRUCT<implementation FLOAT64, datetime TIMESTAMP OPTIONS(description="bq-datetime"), blockNumber INT64>>
)
OPTIONS(
  format="NEWLINE_DELIMITED_JSON",
  uris=["gs://beefy-bi/indexed-data/report/data-coverage.jsonl"]
);
```

## create cleaned tables

```
CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.vault_transfer` AS (
  with all_rows as (
              select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.arbitrum_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.aurora_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.avax_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.bsc_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.celo_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.cronos_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.emerald_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.fantom_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.fuse_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.harmony_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.heco_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.metis_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.moonbeam_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.moonriver_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.optimism_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.polygon_vault_transfer`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.syscoin_vault_transfer`
  )
  SELECT
    split(file_name,'/')[OFFSET(5)] as chain,
    split(file_name,'/')[OFFSET(7)] as contract_address,
    block_number,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', block_datetime) as block_datetime,
    from_address,
    to_address,
    cast(amount as BIGNUMERIC) as moo_amount
  FROM all_rows
);


CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.vault_ppfs_4hour` AS (
  with all_rows as (
              select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.arbitrum_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.aurora_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.avax_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.bsc_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.celo_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.cronos_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.emerald_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.fantom_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.fuse_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.harmony_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.heco_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.metis_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.moonbeam_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.moonriver_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.optimism_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.polygon_ppfs_4hour`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.syscoin_ppfs_4hour`
  )
  SELECT
    split(file_name,'/')[OFFSET(5)] as chain,
    split(file_name,'/')[OFFSET(7)] as contract_address,
    block_number,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', block_datetime) as block_datetime,
    cast(ppfs as BIGNUMERIC) as ppfs
  FROM all_rows
);


CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.oracle_price_15min` AS (
  with all_rows as (
              select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.oracle_price_15min`
  )
  SELECT
    split(file_name,'/')[OFFSET(6)] as oracle_id,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', datetime) as datetime,
    cast(usd_value as BIGNUMERIC) as usd_value
  FROM all_rows
);


CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.beefy_vault` AS (
  with all_rows as (
              select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.all_vaults`
  )
  SELECT
    split(file_name,'/')[OFFSET(5)] as chain,
    id as vault_id,
    token_name,
    token_address,
    token_decimals,
    want_address,
    want_decimals,
    price_oracle.want_oracleId as want_oracle_id,
    price_oracle.assets as internal_assets
  FROM all_rows
);

CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.native_transfer_from` AS (
with all_rows as (
              select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.arbitrum_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.aurora_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.avax_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.bsc_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.celo_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.cronos_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.emerald_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.fantom_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.fuse_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.harmony_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.heco_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.metis_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.moonbeam_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.moonriver_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.optimism_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.polygon_native_transfer_from`
    UNION ALL select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.syscoin_native_transfer_from`
  )
  SELECT
    split(file_name,'/')[OFFSET(5)] as chain,
    split(file_name,'/')[OFFSET(7)] as contract_address,
    block_number,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', block_datetime) as block_datetime,
    cast(amount as BIGNUMERIC) as wnative_amount
  FROM all_rows
);
```

## DERIVED DATA

### moo balance

```

CREATE OR REPLACE TABLE `beefy-bi.beefy_data_derived.investor_moo_balance_ts` AS (with
  -- helper dataset of 1 point each 4h, we'll align all data to this
  regular_ts as (
    select datetime, datetime + INTERVAL 4 HOUR - INTERVAL 1 SECOND as datetime_upper_bound
    from UNNEST(GENERATE_TIMESTAMP_ARRAY('2019-01-01', '2022-07-01', INTERVAL 4 HOUR)) as datetime
  ),
  balance_diff_ind as (
    select transfer.chain, vault.vault_id, transfer.block_datetime, transfer.from_address as investor_address, -moo_amount as moo_amount_diff
    from `beefy-bi.beefy_data_cleaned.vault_transfer` transfer
    join `beefy-bi.beefy_data_cleaned.beefy_vault` vault on transfer.chain = vault.chain and transfer.contract_address = vault.token_address
    where transfer.chain = 'fantom' --and transfer.contract_address = '0x920786cff2A6f601975874Bb24C63f0115Df7dc8'
    UNION ALL
    select transfer.chain, vault.vault_id, transfer.block_datetime, transfer.to_address as investor_address, moo_amount as moo_amount_diff
    from `beefy-bi.beefy_data_cleaned.vault_transfer` transfer
    join `beefy-bi.beefy_data_cleaned.beefy_vault` vault on transfer.chain = vault.chain and transfer.contract_address = vault.token_address
    where transfer.chain = 'fantom' --and transfer.contract_address = '0x920786cff2A6f601975874Bb24C63f0115Df7dc8'
  ),
  empty_ts_by_inv as (
    select *
    from (
      select distinct chain, vault_id, investor_address
      from balance_diff_ind
    ) dims
    cross join regular_ts
  ),
  balance_diff as (
    select chain, vault_id, investor_address, block_datetime, sum(moo_amount_diff) as moo_amount_diff
    from balance_diff_ind
    group by 1,2,3,4
  ),
  investor_ts_gap as (
    select ts.chain, ts.vault_id, ts.investor_address, ts.datetime,
      sum(bal.moo_amount_diff) as moo_amount_diff
    from empty_ts_by_inv ts
    left join balance_diff bal
      on ts.chain = bal.chain
      and ts.vault_id = bal.vault_id
      and ts.investor_address = bal.investor_address
      and bal.block_datetime between ts.datetime and ts.datetime_upper_bound
    group by 1,2,3,4
  ),
  investor_balance_ts_fill as (
    select chain, vault_id, investor_address, datetime,
    coalesce(moo_amount_diff, 0) as moo_amount_diff,
    sum(moo_amount_diff) over W as moo_amount_balance ,
    max(abs(moo_amount_diff)) over W as max_abs_moo_amount_balance
    from investor_ts_gap
    window W as (
      partition by chain, vault_id, investor_address
      order by datetime
      rows between unbounded preceding and current row
    )
  ),
  investor_balance_ts as (
    select chain, vault_id, investor_address, datetime, moo_amount_diff, moo_amount_balance
    from investor_balance_ts_fill
    where max_abs_moo_amount_balance > 0
  )
  select *
  from investor_balance_ts
);
```

### ppfs

```
CREATE OR REPLACE TABLE `beefy-bi.beefy_data_derived.vault_ppfs_ts` AS (
  with
  -- helper dataset of 1 point each 4h, we'll align all data to this
  regular_ts as (
    select datetime, datetime + INTERVAL 4 HOUR - INTERVAL 1 SECOND as datetime_upper_bound
    from UNNEST(GENERATE_TIMESTAMP_ARRAY('2019-01-01', '2022-07-01', INTERVAL 4 HOUR)) as datetime
  ),
  ppfs_w_vault_id as (
    select ppfs.chain, vault.vault_id, vault.want_decimals, vault.want_oracle_id, ppfs.ppfs as ppfs, ppfs.block_datetime
    from `beefy-bi.beefy_data_cleaned.vault_ppfs_4hour` as ppfs
    left join `beefy-bi.beefy_data_cleaned.beefy_vault` vault
      on ppfs.chain = vault.chain
      and ppfs.contract_address = vault.token_address
    --where ppfs.chain = "harmony"
  ),
  empty_ts_by_inv as (
    select *
    from (
      select distinct chain, vault_id
      from ppfs_w_vault_id
    ) dims
    cross join regular_ts
  ),
  ppfs_ts_gap as (
    select ts.chain, ts.vault_id, ts.datetime,
      max(ppfs.want_decimals) as want_decimals,
      max(ppfs.want_oracle_id) as want_oracle_id,
      avg(ppfs.ppfs) as ppfs
    from empty_ts_by_inv ts
    left join ppfs_w_vault_id ppfs
      on ts.chain = ppfs.chain
      and ts.vault_id = ppfs.vault_id
      and ppfs.block_datetime between ts.datetime and ts.datetime_upper_bound
    group by 1,2,3
  ),
  ppfs_ts_fill as (
    select chain, vault_id, datetime,
    -- fill history
    last_value(want_decimals ignore nulls) over W as want_decimals,
    last_value(want_oracle_id ignore nulls) over W as want_oracle_id,
    last_value(ppfs ignore nulls) over W as ppfs,
    max(ppfs) over W as max_ppfs
    from ppfs_ts_gap
    window W as (
      partition by chain, vault_id
      order by datetime asc
      rows between unbounded preceding and current row
    )
  ),
  ppfs_ts as (
    select chain, vault_id, datetime, want_decimals, want_oracle_id, ppfs
    from ppfs_ts_fill
    where max_ppfs > 0
  )
  select *
  from ppfs_ts
);

```

### price

```

CREATE OR REPLACE TABLE `beefy-bi.beefy_data_derived.price_ts` AS (
with
  -- helper dataset of 1 point each 4h, we'll align all data to this
  regular_ts as (
    select datetime, datetime + INTERVAL 4 HOUR - INTERVAL 1 SECOND as datetime_upper_bound
    from UNNEST(GENERATE_TIMESTAMP_ARRAY('2019-01-01', '2022-07-01', INTERVAL 4 HOUR)) as datetime
  ),
  price_rows as (
    select oracle_id, datetime, usd_value
    from `beefy-bi.beefy_data_cleaned.oracle_price_15min` price
  ),
  empty_ts_by_inv as (
    select *
    from (
      select distinct oracle_id
      from price_rows
    ) dims
    cross join regular_ts
  ),
  price_ts_gap as (
    select ts.oracle_id, ts.datetime,
      avg(price.usd_value) as usd_value
    from empty_ts_by_inv ts
    left join price_rows price
      on ts.oracle_id = price.oracle_id
      and price.datetime between ts.datetime and ts.datetime_upper_bound
    group by 1,2
  ),
  price_ts_fill as (
    select oracle_id, datetime,
    -- fill history
    last_value(usd_value ignore nulls) over W as usd_value,
    max(abs(usd_value)) over W as max_abs_usd_value
    from price_ts_gap
    window W as (
      partition by oracle_id
      order by datetime asc
      rows between unbounded preceding and current row
    )
  ),
  price_ts as (
    select *
    from price_ts_fill
    where max_abs_usd_value > 0
  )
  select *
  from price_ts
);


```

## REPORTS

```

CREATE OR REPLACE TABLE `beefy-bi.beefy_data_report.investor_usd_balance_ts` AS (
  with
  investment_individual_values_ts as (
    select
      balance.chain,
      balance.vault_id,
      balance.investor_address,
      balance.datetime,
      balance.moo_amount_balance,
      ppfs.ppfs,
      ppfs.want_decimals,
      price.usd_value as want_usd_value,
    from `beefy-bi.beefy_data_derived.investor_moo_balance_ts` balance
    left join `beefy-bi.beefy_data_derived.vault_ppfs_ts` ppfs
      on balance.chain = ppfs.chain
      and balance.vault_id = ppfs.vault_id
      and balance.datetime = ppfs.datetime
    left join `beefy-bi.beefy_data_derived.price_ts` price
      on price.oracle_id = ppfs.want_oracle_id
      and price.datetime = ppfs.datetime
      and price.datetime = balance.datetime
  ),
  investment_value_ts as (
    select
      *,
      (moo_amount_balance/ POW(CAST(10 as BIGNUMERIC), CAST((want_decimals + 18 /* moo decimals */)  as BIGNUMERIC))) * ppfs as investor_want_amount,
      (
        (
          (moo_amount_balance/ POW(CAST(10 as BIGNUMERIC), CAST((want_decimals + 18 /* moo decimals */)  as BIGNUMERIC))) * ppfs
        )
        * want_usd_value
      ) as investor_usd_amount
      from investment_individual_values_ts
  )
select *
from investment_value_ts
order by chain, vault_id, investor_address, datetime
);



CREATE OR REPLACE TABLE `beefy-bi.beefy_data_report.vault_data_coverage` AS (
 select *
 from `beefy-bi.beefy_data_raw.all_vault_data_coverage`
);
```
