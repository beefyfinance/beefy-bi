```
erc20_transfer_ts
vault_ppfs_ts
oracle_price_ts

-- check compression
select pg_size_pretty(hypertable_size('data_raw.'));
SELECT * FROM hypertable_compression_stats('data_raw.');

-- flex your compression
SELECT pg_size_pretty(before_compression_total_bytes) as "before compression", pg_size_pretty(after_compression_total_bytes) as "after compression" FROM hypertable_compression_stats('data_raw.');

-- find a compression job to run
SELECT * FROM timescaledb_information.jobs;
SELECT * FROM timescaledb_information.job_stats;

-- run a compression job
CALL run_job(1000);



-- get a table and continuous agg table size reports
with timescale_objects as (
    select
        'hypertable' as object_type,
        hypertable_schema || '.' || hypertable_name as table_display_name,
        hypertable_schema || '.' || hypertable_name as table_internal_name
    from timescaledb_information.hypertables
    UNION ALL
    select
        'continuous aggregate' as object_type,
        hypertable_schema || '.' || hypertable_name as table_display_name,
        materialization_hypertable_schema || '.' || materialization_hypertable_name as table_internal_name
    from timescaledb_information.continuous_aggregates
)
select ht.object_type, ht.table_display_name, ht.table_internal_name,
    pg_size_pretty(htds.table_bytes) as table_size,
    pg_size_pretty(htds.index_bytes) as index_bytes,
    pg_size_pretty(htds.toast_bytes) as toast_bytes,
    pg_size_pretty(htds.total_bytes) as total_bytes,
    htds.total_bytes as raw_total_bytes
from timescale_objects as ht,
lateral (
    select
        sum(table_bytes) as table_bytes,
        sum(index_bytes) as index_bytes,
        sum(toast_bytes) as toast_bytes,
        sum(total_bytes) as total_bytes
    from hypertable_detailed_size(ht.table_internal_name)
) as htds
order by htds.total_bytes desc
;

with timescale_objects as (
    select
        'hypertable' as object_type,
        hypertable_schema || '.' || hypertable_name as table_display_name,
        hypertable_schema || '.' || hypertable_name as table_internal_name
    from timescaledb_information.hypertables
    UNION ALL
    select
        'continuous aggregate' as object_type,
        hypertable_schema || '.' || hypertable_name as table_display_name,
        materialization_hypertable_schema || '.' || materialization_hypertable_name as table_internal_name
    from timescaledb_information.continuous_aggregates
)
select *
from information_schema.tables
where table_schema not like '%timescaledb%'
    and table_schema not in ('pg_catalog', 'information_schema')
    and table_schema || '.' || table_name not in (
        select table_internal_name from timescale_objects
        UNION ALL
        select table_display_name from timescale_objects
    );


-- Full refresh all continuous aggregates
CALL refresh_continuous_aggregate('data_derived.vault_ppfs_4h_ts', '2018-01-01', now());
CALL refresh_continuous_aggregate('data_derived.oracle_price_4h_ts', '2018-01-01', now());
CALL refresh_continuous_aggregate('data_derived.erc20_owner_balance_diff_4h_ts', '2018-01-01', now());
CALL refresh_continuous_aggregate('data_derived.erc20_contract_balance_diff_4h_ts', '2018-01-01', now());
CALL refresh_continuous_aggregate('data_report.chain_stats_4h_ts', '2018-01-01', now());
CALL refresh_continuous_aggregate('data_report.all_stats_4h_ts', '2018-01-01', now());



select
    schemaname, relname,
      pg_size_pretty(pg_total_relation_size(relid)) as total_size,
      pg_size_pretty(pg_relation_size(relid, 'main')) as relation_size_main,
      pg_size_pretty(pg_relation_size(relid, 'fsm')) as relation_size_fsm,
      pg_size_pretty(pg_relation_size(relid, 'vm')) as relation_size_vm,
      pg_size_pretty(pg_relation_size(relid, 'init')) as relation_size_init,
      pg_size_pretty(pg_table_size(relid)) as table_size,
      pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as external_size
 from
      pg_catalog.pg_statio_user_tables
where schemaname not in ('_timescaledb_config', '_timescaledb_internal', '_timescaledb_catalog', '_timescaledb_cache')
order by pg_total_relation_size(relid) desc;






select datetime,
    shares.block_number,
    shares.chain,
    bytea_to_hexstr(trx.hash) as trx_hash,
    bytea_to_hexstr(owner.address) as owner,
    bytea_to_hexstr(vault.address) as vault,
    shares.shares_balance_diff,
    shares.shares_balance_after
from vault_shares_transfer_ts shares
    join evm_address owner on shares.owner_evm_address_id = owner.evm_address_id
    join evm_address vault on shares.vault_evm_address_id = vault.evm_address_id
    join evm_transaction trx on shares.evm_transaction_id = trx.evm_transaction_id
order by 1, 2, 3, 4, 5;


with
products_with_price as (
  select product_id
  from product
  where price_feed_id in (
    select price_feed_id
    from asset_price_ts
  )
),
last_balance as (
  select
    investor_id,
    last(balance, datetime) as balance
  from investment_balance_ts
  where product_id in (
    select product_id from products_with_price
  )
  group by investor_id
),
positive_balance as (
  select *
  from last_balance
  where balance > 0
)
select investor_id, bytea_to_hexstr(address) as address
from investor
where investor_id in (select investor_id from positive_balance);



select
    p.chain,
    b.*
from
    investment_balance_ts b
    join product p on b.product_id = p.product_id
where
    p.product_key ~* 'curve-arb-tricrypto'
order by
    datetime desc
limit 5;

select
  p.chain,
  b.datetime,
  b.block_number,
  p.product_key,
  i.address,
  b.balance,
  usd.usd_value,
  b.balance * usd.usd_value as investment_usd_value,
  b.investment_data
from
  investment_balance_ts b
  join product p on b.product_id = p.product_id
  join investor i on b.investor_id = i.investor_id
  left join asset_price_ts usd on usd.price_feed_id = p.price_feed_id
  and b.datetime between usd.datetime - '15min' :: interval
  and usd.datetime
where
  investment_data->>'trxHash' = '0xa6df9dec14ae250435a2dae5f9b4886a5a440dfc89c45d6a3ef485fc9be27af5'
order by
  b.datetime desc;


```

```
\c postgres

create database beefy_bck with template beefy;
-- if needed
SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE pid <> pg_backend_pid()
               AND datname IS NOT NULL
               AND leader_pid IS NULL;

\c beefy

-- migrate price feed table
alter table price_feed add column from_asset_key character varying;
alter table price_feed add column to_asset_key character varying;
update price_feed set from_asset_key = case when external_id like '%-%' then feed_key else external_id end;
update price_feed set to_asset_key = 'fiat:USD';
alter table price_feed add column _feed_data jsonb;
update price_feed set _feed_data = jsonb_build_object('active', price_feed_data->'is_active');
alter table price_feed alter column _feed_data set not null;
alter table price_feed drop column price_feed_data;
alter table price_feed rename column _feed_data to price_feed_data;

alter table asset_price_ts rename to price_ts;
alter table price_ts rename column usd_value to price;

alter table import_status rename to product_import;
update product_import set import_data = jsonb_build_object(
    'contractCreatedAtBlock', import_data->'data'->'contractCreatedAtBlock',
    'chainLatestBlockNumber', import_data->'data'->'chainLatestBlockNumber',
    'ranges', jsonb_build_object(
        'lastImportDate', import_data->'data'->'lastImportDate',
        'coveredRanges', import_data->'data'->'coveredBlockRanges',
        'toRetry', import_data->'data'->'blockRangesToRetry'
    )
);
```
