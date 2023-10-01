-- New product without import state
select count(*)
from product p
where p.product_id not in (
  select (i.import_data->'productId')::integer 
  from import_state i
  where i.import_data->>'type' = 'product:investment'
)

-- too many cronos errors (+500% last 1w)
with
errors_current_period as (
  SELECT
    time_bucket('15min', datetime) AS "time",
    count(*) as c
  FROM rpc_error_ts 
  WHERE
    time_bucket('15min', datetime) BETWEEN $__timeFrom() AND $__timeTo()
    and chain in ('cronos')
  group by 1
),
errors_previous_period as (
  SELECT
    time_bucket('15min', datetime) AS "time",
    count(*) as c
  FROM rpc_error_ts 
  WHERE
    time_bucket('15min', datetime) BETWEEN 
      ($__timeFrom()::timestamptz - ($__timeTo()::timestamptz - $__timeFrom()::timestamptz)::interval) 
      AND ($__timeTo()::timestamptz - ($__timeTo()::timestamptz - $__timeFrom()::timestamptz)::interval)
    and chain in ('cronos')
  group by 1
)
select (avg(c.c) - avg(p.c)) / avg(p.c)
from errors_current_period c, errors_previous_period p


-- too many errors (+20% last 6h)
with
errors_current_period as (
  SELECT
    time_bucket('15min', datetime) AS "time",
    count(*) as c
  FROM rpc_error_ts 
  WHERE
    time_bucket('15min', datetime) BETWEEN $__timeFrom() AND $__timeTo()
    and chain not in ('cronos')
  group by 1
),
errors_previous_period as (
  SELECT
    time_bucket('15min', datetime) AS "time",
    count(*) as c
  FROM rpc_error_ts 
  WHERE
    time_bucket('15min', datetime) BETWEEN 
      ($__timeFrom()::timestamptz - ($__timeTo()::timestamptz - $__timeFrom()::timestamptz)::interval) 
      AND ($__timeTo()::timestamptz - ($__timeTo()::timestamptz - $__timeFrom()::timestamptz)::interval)
    and chain not in ('cronos')
  group by 1
)
select (avg(c.c) - avg(p.c)) / avg(p.c)
from errors_current_period c, errors_previous_period p

-- Too many price missing in cache table
select count(*) price_cache_without_price
from beefy_investor_timeline_cache_ts 
where underlying_to_usd_price is null

-- Work Amount is raising up
with total_work_by_chain as (
  SELECT
    chain,
    datetime,
    --sum(errors_count) as errors_count,
    --success_count as success_count,
    --sum(not_covered_yet) as not_covered_yet,
    sum(errors_count) + sum(not_covered_yet) as total_work
  FROM import_state_metrics_ts
  WHERE
    $__timeFilter(datetime)
    --and chain in ('bsc')
    and import_type in ('product:investment', 'product:share-rate')
  group by 1, 2
)
select datetime AS "time", sum(total_work) as total_work
from total_work_by_chain
group by 1
ORDER BY 2