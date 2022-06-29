# UTILS

## export ddl of a manually created table

```
SELECT
 table_name, ddl
FROM
 `beefy-bi.beefy_data_raw.INFORMATION_SCHEMA.TABLES`
```

# RAW TABLES

## manual create

```
beefy-bi/indexed-data/chain/CHAIN/contracts/*/ERC20/Transfer.csv

CHAIN_vault_transfer

external table

[
    {
        "name": "block_number",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "block_datetime",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "from_address",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "to_address",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "amount",
        "type": "BIGNUMERIC",
        "mode": "REQUIRED"
    }
]
```

## SQL create

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

CREATE OR REPLACE EXTERNAL TABLE `beefy-bi.beefy_data_raw.all_vaults`
(
  id STRING NOT NULL,
  token_name STRING NOT NULL,
  token_decimals INTEGER NOT NULL,
  token_address STRING NOT NULL,
  want_address STRING NOT NULL,
  want_decimals INTEGER NOT NULL,
  price_oracle JSON NOT NULL
)
OPTIONS(
  format="NEWLINE_DELIMITED_JSON",
  uris=["gs://beefy-bi/indexed-data/chain/*/beefy/vaults.jsonl"]
);
```

```
CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.vault_transfer` AS (
  with all_transfers as (
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
    split(file_name,'/')[OFFSET(4)] as chain,
    split(file_name,'/')[OFFSET(6)] as contract_address,
    block_number,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', block_datetime) as block_datetime,
    from_address,
    to_address,
    amount / (POW(10, 18)) as amount
  FROM all_transfers
);


CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.vault_ppfs_4hour` AS (
  with all_ppfs as (
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
    split(file_name,'/')[OFFSET(4)] as chain,
    split(file_name,'/')[OFFSET(6)] as contract_address,
    block_number,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', block_datetime) as block_datetime,
    ppfs / (POW(10, 18)) as ppfs
  FROM all_ppfs
);


CREATE OR REPLACE TABLE `beefy-bi.beefy_data_cleaned.oracle_price_15min` AS (
  with all_prices as (
              select _FILE_NAME as file_name, * FROM `beefy-bi.beefy_data_raw.oracle_price_15min`
  )
  SELECT
    split(file_name,'/')[OFFSET(6)] as oracle_id,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', datetime) as datetime,
    usd_value
  FROM all_prices
);
```
