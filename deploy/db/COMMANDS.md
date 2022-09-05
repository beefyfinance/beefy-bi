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
    pg_size_pretty(htds.total_bytes) as total_bytes
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
order by pg_total_relation_size(relid) desc;






select datetime,
    shares.block_number,
    shares.chain,
    trx.hash,
    owner.address as owner,
    vault.address as vault,
    shares.shares_balance_diff,
    shares.shares_balance_after
from vault_shares_transfer_ts shares
    join evm_address owner on shares.owner_evm_address_id = owner.evm_address_id
    join evm_address vault on shares.vault_evm_address_id = vault.evm_address_id
    join evm_transaction trx on shares.evm_transaction_id = trx.evm_transaction_id;



with balance_gf_ts as (
  SELECT
    time_bucket_gapfill('15min', datetime) as datetime,
    vault_evm_address_id,
    locf(last(shares_balance_after::numeric, datetime)) as shares_balance_after
  from vault_shares_transfer_ts shares
  WHERE
    $__timeFilter(datetime)
    and owner_evm_address_id = $investor_id
  GROUP BY 1, 2
),
balance_ts as (
  select * from balance_gf_ts where shares_balance_after is not null
),
shares_gf_ts as (
  select
    time_bucket_gapfill('15min', datetime) as datetime,
    vault_evm_address_id,
    locf(last(shares_to_underlying_rate::numeric, datetime)) as shares_to_underlying_rate
  from vault_to_underlying_rate_ts
  WHERE
    $__timeFilter(datetime)
    and vault_evm_address_id in (select vault_evm_address_id from balance_ts)
  group by 1,2
),
shares_ts as (
  select * from shares_gf_ts where shares_to_underlying_rate is not null
),
price_gf_ts as (
  select
    time_bucket_gapfill('15min', datetime) as datetime,
    v.contract_evm_address_id as vault_evm_address_id,
    locf(last(usd_value, datetime)) as usd_value
  from asset_price_ts p
    join evm_address u on p.price_feed_key = metadata->'erc20'->>'price_feed_key'
    join beefy_vault v on u.evm_address_id = v.underlying_evm_address_id
  where
    $__timeFilter(datetime)
    and v.contract_evm_address_id in (
      select vault_evm_address_id
      from balance_ts
    )
  group by 1,2
),
price_ts as (
  select * from price_gf_ts where usd_value is not null
)
select
  b.datetime as time,
  a.metadata->'erc20'->>'price_feed_key' as vault_name,
  b.shares_balance_after,
  s.shares_to_underlying_rate,
  p.usd_value,
  (b.shares_balance_after * s.shares_to_underlying_rate * p.usd_value) as investor_balance_usd_value
from balance_ts b
  left join shares_ts s on b.datetime = s.datetime and b.vault_evm_address_id = s.vault_evm_address_id
  left join price_ts p on b.datetime = p.datetime and b.vault_evm_address_id = p.vault_evm_address_id
  left join evm_address a on b.vault_evm_address_id = a.evm_address_id
order by 1;



```
