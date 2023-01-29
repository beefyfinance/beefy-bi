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
  left join price_ts usd on usd.price_feed_id = p.price_feed_id
  and b.datetime between usd.datetime - '15min' :: interval
  and usd.datetime
where
  investment_data->>'trxHash' = '0xa6df9dec14ae250435a2dae5f9b4886a5a440dfc89c45d6a3ef485fc9be27af5'
order by
  b.datetime desc;


```

```
\c postgres
drop database beefy_bck;
create database beefy_bck with template beefy;
-- if needed
SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE pid <> pg_backend_pid()
               AND datname IS NOT NULL
               AND leader_pid IS NULL;

\c beefy

update investment_balance_ts set balance = (investment_data->>'balance')::evm_decimal_256;







INSERT INTO block_ts (
    datetime,
    chain,
    block_number,
    block_data
) (
    select b.datetime, p.chain, b.block_number, '{}'::jsonb
    from investment_balance_ts b
        join product p on p.product_id = b.product_id
    UNION ALL
    select distinct
        (i.import_data->>'contractCreationDate')::timestamptz,
        (i.import_data->>'chain')::chain_enum,
        (i.import_data->>'contractCreatedAtBlock')::integer,
        '{}'::jsonb
    from import_state i
    where i.import_data->>'contractCreationDate' is not null
)
on conflict do nothing;


create table test as
 select  (investment_data->>'balanceDiff')::evm_decimal_256  as v from investment_balance_ts;



SELECT hypertable_name, pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass))
  FROM timescaledb_information.hypertables;


begin;
alter table investment_balance_ts rename column investment_data to _investment_data;
alter table investment_balance_ts add column pending_rewards NUMERIC(100, 24);
alter table investment_balance_ts add column pending_rewards_diff NUMERIC(100, 24);
alter table investment_balance_ts add column investment_data jsonb;
update investment_balance_ts set investment_data = _investment_data;
alter table investment_balance_ts alter column investment_data set not null;
alter table investment_balance_ts drop column _investment_data;
--update investment_balance_ts set pending_rewards = 0, pending_rewards_diff = 0;
--alter table investment_balance_ts alter column pending_rewards set not null;
--alter table investment_balance_ts alter column pending_rewards set not null;
alter table investment_balance_ts alter column pending_rewards set data type evm_decimal_256_nullable;
alter table investment_balance_ts alter column pending_rewards_diff set data type evm_decimal_256_nullable;


alter table product rename column product_data to _product_data;
alter table product add column pending_rewards_price_feed_id integer null references price_feed(price_feed_id);
alter table product add column product_data jsonb;
update product set product_data = _product_data;
alter table product alter column product_data set not null;
alter table product drop column _product_data;
commit;


```

```sql
BEGIN;
alter table investment_balance_ts add column debug_data_uuid uuid not null default uuid_generate_v4();
insert into debug_data_ts (datetime, origin_table, debug_data_uuid, debug_data)
select datetime, 'investment_balance_ts', debug_data_uuid, investment_data from investment_balance_ts;
alter table investment_balance_ts drop column investment_data;
alter table investment_balance_ts alter column debug_data_uuid drop default;

alter table price_ts add column debug_data_uuid uuid not null default uuid_generate_v4();
insert into debug_data_ts (datetime, origin_table, debug_data_uuid, debug_data)
select datetime, 'price_ts', debug_data_uuid, price_data from price_ts;
alter table price_ts drop column price_data;
alter table price_ts alter column debug_data_uuid drop default;

alter table block_ts add column debug_data_uuid uuid not null default uuid_generate_v4();
insert into debug_data_ts (datetime, origin_table, debug_data_uuid, debug_data)
select datetime, 'block_ts', debug_data_uuid, block_data from block_ts;
alter table block_ts drop column block_data;
alter table block_ts alter column debug_data_uuid drop default;
COMMIT;

vacuum full analyze price_ts;
vacuum full analyze block_ts;
vacuum full analyze investment_balance_ts;


```

```text

-- BEFORE
SELECT hypertable_name, pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass))
  FROM timescaledb_information.hypertables;
    hypertable_name    | pg_size_pretty
-----------------------+----------------
 investment_balance_ts | 14 GB
 block_ts              | 4341 MB
 price_ts              | 45 GB
(3 rows)

-- AFTER
SELECT hypertable_name, pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass))
  FROM timescaledb_information.hypertables;
    hypertable_name    | pg_size_pretty
-----------------------+----------------
 investment_balance_ts | 5763 MB
 block_ts              | 4050 MB
 price_ts              | 23 GB
 debug_data_ts         | 49 GB
(4 rows)

-- ENABLING COMPRESSION ON DEBUG DATA
SELECT hypertable_name, pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass))
  FROM timescaledb_information.hypertables;
    hypertable_name    | pg_size_pretty
-----------------------+----------------
 investment_balance_ts | 5763 MB
 block_ts              | 4050 MB
 price_ts              | 23 GB
 debug_data_ts         | 32 GB
(4 rows)
```

```sql
-- cleanup all loose relationships in the database
delete
from import_state
where import_key ~* '^product:investment:[0-9]+$'
and import_key not in (
  select 'product:investment:' || product_id
  from product
);

delete
from import_state
where import_key ~* '^product:investment:pending-reward:[0-9]+:[0-9]+$'
and regexp_replace(import_key, ':[0-9]+$', '') not in (
  select 'product:investment:pending-reward:' || product_id
  from product
);


alter table import_state add column keep boolean not null default true;
update import_state set keep = false
  where import_key ~* '^product:investment:pending-reward:[0-9]+:[0-9]+$'
  and regexp_replace(import_key, ':[0-9]+$', '') not in (
    select 'product:investment:pending-reward:' || product_id
    from product
  );
delete from import_state where not keep;
alter table import_state drop column keep;


delete from price_ts where price_feed_id not in (
  select price_feed_1_id from product
  union all
  select price_feed_2_id from product
  union all
  select pending_rewards_price_feed_id from product where pending_rewards_price_feed_id is not null
);

delete from price_feed where price_feed_id not in (
  select price_feed_1_id from product
  union all
  select price_feed_2_id from product
  union all
  select pending_rewards_price_feed_id from product where pending_rewards_price_feed_id is not null
);

delete from import_state
where import_key ~* '^price:feed:[0-9]+$'
and import_key not in (
  select 'price:feed:' || price_feed_id
  from price_feed
);

-- create a new table to avoid mvcc issues
-- this has to be the only thing running
alter table debug_data_ts rename to debug_data_ts_old;
alter index debug_data_ts_uuid_idx rename to debug_data_ts_old_uuid_idx;
CREATE TABLE IF NOT EXISTS debug_data_ts (
  debug_data_uuid uuid not null, -- unique id, use uuids so we can generate them without having to query the db
  datetime timestamptz not null, -- any datetime to make it a hypertable
  origin_table debug_origin_table_enum not null, -- the table this debug data comes from
  debug_data jsonb not null -- the actual debug data
);
create index debug_data_ts_uuid_idx on debug_data_ts (origin_table, debug_data_uuid);
SELECT create_hypertable(
  relation => 'debug_data_ts',
  time_column_name => 'datetime',
  chunk_time_interval => INTERVAL '30 days',
  if_not_exists => true
);


DO
$$
DECLARE
  refresh_interval INTERVAL = '1d'::INTERVAL;
  start_timestamp TIMESTAMPTZ = '2020-09-01T00:00:00Z'::TIMESTAMPTZ;
  end_timestamp TIMESTAMPTZ = start_timestamp + refresh_interval;
BEGIN
  WHILE start_timestamp < now() LOOP
    insert into debug_data_ts (datetime, origin_table, debug_data_uuid, debug_data) (
      select t.datetime, 'investment_balance_ts', t.debug_data_uuid, d.debug_data
      from investment_balance_ts t
      join debug_data_ts_old d on d.debug_data_uuid = t.debug_data_uuid
      where d.origin_table = 'investment_balance_ts'
      and t.datetime >= start_timestamp and t.datetime < end_timestamp
    );
    RAISE NOTICE 'finished with timestamp %', end_timestamp;
    start_timestamp = end_timestamp;
    end_timestamp = end_timestamp + refresh_interval;
  END LOOP;
END
$$;


DO
$$
DECLARE
  refresh_interval INTERVAL = '1d'::INTERVAL;
  start_timestamp TIMESTAMPTZ = '2020-09-01T00:00:00Z'::TIMESTAMPTZ;
  end_timestamp TIMESTAMPTZ = start_timestamp + refresh_interval;
BEGIN
  WHILE start_timestamp < now() LOOP
    insert into debug_data_ts (datetime, origin_table, debug_data_uuid, debug_data) (
      select t.datetime, 'price_ts', t.debug_data_uuid, d.debug_data
      from price_ts t
      join debug_data_ts_old d on d.debug_data_uuid = t.debug_data_uuid
      where d.origin_table = 'price_ts'
      and t.datetime >= start_timestamp and t.datetime < end_timestamp
    );
    RAISE NOTICE 'finished with timestamp %', end_timestamp;
    start_timestamp = end_timestamp;
    end_timestamp = end_timestamp + refresh_interval;
  END LOOP;
END
$$;


DO
$$
DECLARE
  refresh_interval INTERVAL = '1d'::INTERVAL;
  start_timestamp TIMESTAMPTZ = '2020-09-01T00:00:00Z'::TIMESTAMPTZ;
  end_timestamp TIMESTAMPTZ = start_timestamp + refresh_interval;
BEGIN
  WHILE start_timestamp < now() LOOP
    insert into debug_data_ts (datetime, origin_table, debug_data_uuid, debug_data) (
      select t.datetime, 'block_ts', t.debug_data_uuid, d.debug_data
      from block_ts t
      join debug_data_ts_old d on d.debug_data_uuid = t.debug_data_uuid
      where d.origin_table = 'block_ts'
      and t.datetime >= start_timestamp and t.datetime < end_timestamp
    );
    RAISE NOTICE 'finished with timestamp %', end_timestamp;
    start_timestamp = end_timestamp;
    end_timestamp = end_timestamp + refresh_interval;
  END LOOP;
END
$$;

-- recompress

ALTER TABLE debug_data_ts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'origin_table'
);
SELECT add_compression_policy('debug_data_ts', INTERVAL '7 days');
select compress_chunk(c)
  from show_chunks('debug_data_ts') as c;




-- cleanup ignored addresses
delete from investment_balance_ts where investor_id in (
  select i.investor_id
  from ignore_address ia
  join investor i on ia.address = i.address
);


delete from investor where investor_id in (
  select i.investor_id
  from ignore_address ia
  join investor i on ia.address = i.address
);


-- reclaim some space
vacuum full analyze price_ts;
vacuum full analyze block_ts;
vacuum full analyze investment_balance_ts;
vacuum full analyze debug_data_ts;
```

```sql
insert into beefy_investor_timeline_cache_ts (
  investor_id,
  product_id,
  datetime,
  block_number,
  price_feed_1_id,
  price_feed_2_id,
  pending_rewards_price_feed_id,
  balance,
  balance_diff,
  share_to_underlying_price,
  underlying_balance,
  underlying_diff,
  underlying_to_usd_price,
  usd_balance,
  usd_diff,
  pending_rewards,
  pending_rewards_diff
) (
  with investment_diff_raw as (
    select b.datetime, b.block_number, b.investor_id, b.product_id,
      p.price_feed_1_id,
      p.price_feed_2_id,
      p.pending_rewards_price_feed_id,
      last(b.balance, b.datetime) as balance,
      sum(b.balance_diff) as balance_diff,
      last(pr1.price::numeric, pr1.datetime) as price1,
      last(pr2.price::numeric, pr2.datetime) as price2,
      last(b.pending_rewards, b.datetime) as pending_reward,
      sum(b.pending_rewards_diff) as pending_reward_diff
    from investment_balance_ts b
    left join product p
      on b.product_id = p.product_id
    -- we should have the exact price1 (share to underlying) from this exact block for all investment change
    left join price_ts pr1
      on p.price_feed_1_id = pr1.price_feed_id
      and pr1.datetime = b.datetime
      and pr1.block_number = b.block_number
    -- but for price 2 (underlying to usd) we need to match on approx time
    left join price_ts pr2
      on p.price_feed_2_id = pr2.price_feed_id
      and time_bucket('15min', pr2.datetime) = time_bucket('15min', b.datetime)
    where b.balance_diff != 0 -- only show changes, not reward snapshots
    group by 1,2,3,4,5,6,7
    having sum(b.balance_diff) != 0 -- only show changes, not reward snapshots
  )
  select
    b.investor_id,
    b.product_id,
    b.datetime,
    b.block_number,
    b.price_feed_1_id,
    b.price_feed_2_id,
    b.pending_rewards_price_feed_id,
    b.balance as balance,
    b.balance_diff as balance_diff,
    b.price1 as share_to_underlying_price,
    (b.balance * b.price1)::NUMERIC(100, 24) as underlying_balance,
    (b.balance_diff * b.price1)::NUMERIC(100, 24) as underlying_diff,
    b.price2 as underlying_to_usd_price,
    (b.balance * b.price1 * b.price2)::NUMERIC(100, 24) as usd_balance,
    (b.balance_diff * b.price1 * b.price2)::NUMERIC(100, 24) as usd_diff,
    b.pending_reward as pending_reward,
    b.pending_reward_diff as pending_reward_diff
  from investment_diff_raw b
  join product p on p.product_id = b.product_id
)
ON CONFLICT (product_id, investor_id, block_number, datetime)
DO UPDATE SET
    balance = coalesce(EXCLUDED.balance, beefy_investor_timeline_cache_ts.balance),
    balance_diff = coalesce(EXCLUDED.balance_diff, beefy_investor_timeline_cache_ts.balance_diff),
    share_to_underlying_price = coalesce(EXCLUDED.share_to_underlying_price, beefy_investor_timeline_cache_ts.share_to_underlying_price),
    underlying_to_usd_price = coalesce(EXCLUDED.underlying_to_usd_price, beefy_investor_timeline_cache_ts.underlying_to_usd_price),
    underlying_balance = coalesce(EXCLUDED.underlying_balance, beefy_investor_timeline_cache_ts.underlying_balance),
    underlying_diff = coalesce(EXCLUDED.underlying_diff, beefy_investor_timeline_cache_ts.underlying_diff),
    pending_rewards = coalesce(EXCLUDED.pending_rewards, beefy_investor_timeline_cache_ts.pending_rewards),
    pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, beefy_investor_timeline_cache_ts.pending_rewards_diff);


```

```sql




select b.investor_id, p.product_id, p.chain, i.address, count(*)
from investment_balance_ts b
left join product p on b.product_id = p.product_id
left join investor i on b.investor_id = i.investor_id
where balance_diff != 0
group by 1,2,3,4
order by count(*) desc
limit 20;

```

```sql


insert into beefy_investor_timeline_cache_ts (
  investor_id,
  product_id,
  datetime,
  block_number,
  price_feed_1_id,
  price_feed_2_id,
  pending_rewards_price_feed_id,
  balance,
  balance_diff,
  share_to_underlying_price,
  underlying_balance,
  underlying_diff,
  underlying_to_usd_price,
  usd_balance,
  usd_diff,
  pending_rewards,
  pending_rewards_diff
) (
  with balance_scope as (
    select *
    from beefy_investor_timeline_cache_ts
    where underlying_to_usd_price is null
  ),
  investment_diff_raw as (
    select b.datetime, b.block_number, b.investor_id, b.product_id,
      p.price_feed_1_id,
      p.price_feed_2_id,
      p.pending_rewards_price_feed_id,
      last(b.balance, b.datetime) as balance,
      sum(b.balance_diff) as balance_diff,
      last(pr1.price::numeric, pr1.datetime) as price1,
      last(pr2.price::numeric, pr2.datetime) as price2,
      last(b.pending_rewards, b.datetime) as pending_reward,
      sum(b.pending_rewards_diff) as pending_reward_diff
    from balance_scope b
     join product p
      on b.product_id = p.product_id
    -- we should have the exact price1 (share to underlying) from this exact block for all investment change
    join price_ts pr1
      on p.price_feed_1_id = pr1.price_feed_id
      and pr1.datetime = b.datetime
      and pr1.block_number = b.block_number
    -- but for price 2 (underlying to usd) we need to match on approx time
    join price_ts pr2
      on p.price_feed_2_id = pr2.price_feed_id
      and time_bucket('15min', pr2.datetime) = time_bucket('15min', b.datetime)
    where b.balance_diff != 0 -- only show changes, not reward snapshots
    group by 1,2,3,4,5,6,7
    having sum(b.balance_diff) != 0 -- only show changes, not reward snapshots
  )
  select
    b.investor_id,
    b.product_id,
    b.datetime,
    b.block_number,
    b.price_feed_1_id,
    b.price_feed_2_id,
    b.pending_rewards_price_feed_id,

    b.balance as balance,
    b.balance_diff as balance_diff,
    b.price1 as share_to_underlying_price,
    (b.balance * b.price1)::NUMERIC(100, 24) as underlying_balance,
    (b.balance_diff * b.price1)::NUMERIC(100, 24) as underlying_diff,
    b.price2 as underlying_to_usd_price,
    (b.balance * b.price1 * b.price2)::NUMERIC(100, 24) as usd_balance,
    (b.balance_diff * b.price1 * b.price2)::NUMERIC(100, 24) as usd_diff,
    b.pending_reward as pending_reward,
    b.pending_reward_diff as pending_reward_diff
  from investment_diff_raw b
  join product p on p.product_id = b.product_id
)
ON CONFLICT (product_id, investor_id, block_number, datetime)
DO UPDATE SET
    balance = coalesce(EXCLUDED.balance, beefy_investor_timeline_cache_ts.balance),
    balance_diff = coalesce(EXCLUDED.balance_diff, beefy_investor_timeline_cache_ts.balance_diff),
    share_to_underlying_price = coalesce(EXCLUDED.share_to_underlying_price, beefy_investor_timeline_cache_ts.share_to_underlying_price),
    underlying_to_usd_price = coalesce(EXCLUDED.underlying_to_usd_price, beefy_investor_timeline_cache_ts.underlying_to_usd_price),
    underlying_balance = coalesce(EXCLUDED.underlying_balance, beefy_investor_timeline_cache_ts.underlying_balance),
    underlying_diff = coalesce(EXCLUDED.underlying_diff, beefy_investor_timeline_cache_ts.underlying_diff),
    pending_rewards = coalesce(EXCLUDED.pending_rewards, beefy_investor_timeline_cache_ts.pending_rewards),
    pending_rewards_diff = coalesce(EXCLUDED.pending_rewards_diff, beefy_investor_timeline_cache_ts.pending_rewards_diff);

```

```sql






explain update beefy_investor_timeline_cache_ts set
  underlying_to_usd_price = pr2.price,
  usd_balance = underlying_balance * pr2.price,
  usd_diff = underlying_diff * pr2.price
from price_ts pr2
where underlying_to_usd_price is null and investor_id = 8402782
  and beefy_investor_timeline_cache_ts.price_feed_2_id = pr2.price_feed_id
  and time_bucket('15min', pr2.datetime) = time_bucket('15min', beefy_investor_timeline_cache_ts.datetime);

explain with missing_cache_prices as materialized (
  select
    c.investor_id,
    c.product_id,
    c.datetime,
    c.block_number,
    last(pr2.price, pr2.datetime) as price
  from beefy_investor_timeline_cache_ts c
    join price_ts pr2 on c.price_feed_2_id = pr2.price_feed_id
     and time_bucket('15min', pr2.datetime) = time_bucket('15min', c.datetime)
  where c.underlying_to_usd_price is null
  group by 1,2,3,4
)
update beefy_investor_timeline_cache_ts
set
  underlying_to_usd_price = to_update.price,
  usd_balance = underlying_balance * to_update.price,
  usd_diff = underlying_diff * to_update.price
from missing_cache_prices to_update
where to_update.investor_id = beefy_investor_timeline_cache_ts.investor_id
  and to_update.product_id = beefy_investor_timeline_cache_ts.product_id
  and to_update.datetime = beefy_investor_timeline_cache_ts.datetime
  and to_update.block_number = beefy_investor_timeline_cache_ts.block_number;




```

```sql
 call refresh_continuous_aggregate('price_ts_cagg_1h', now() - '1 month'::interval, now());

  call refresh_continuous_aggregate('price_ts_cagg_1d', now() - '18 month'::interval, now());
```
