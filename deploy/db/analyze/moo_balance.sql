--explain 
--explain analyze
with 
erc20_transfer_scope as (
    SELECT *
    FROM beefy_raw.erc20_transfer_ts
    where chain = 'bsc'
),
moo_balance_diff_ts as (
    select chain, contract_address, investor_address,
        time_bucket_gapfill('1d', datetime) as datetime,
        sum(moo_balance_diff) as moo_balance_diff,
        sum(sum(moo_balance_diff)) over(
            partition by chain, contract_address, investor_address
            order by time_bucket_gapfill('1d', datetime)
        ) as moo_balance
    from (
        SELECT chain, contract_address, from_address as investor_address, 
            time_bucket('1d', datetime) as datetime,
            sum(value * -1) as moo_balance_diff
        FROM erc20_transfer_scope
        group by 1,2,3,4
        UNION ALL
        SELECT chain, contract_address, to_address as investor_address, 
            time_bucket('1d', datetime) as datetime, 
            sum(value * 1) as moo_balance_diff
        FROM erc20_transfer_scope
        group by 1,2,3,4
    ) as investor_diffs
    where datetime between now() - INTERVAL '2 year' and now()
    group by 1,2,3,4
),
moo_balance_ts as (
    select chain, contract_address, investor_address, datetime, moo_balance_diff, moo_balance
    from moo_balance_diff_ts
    where moo_balance is not null
)
select count(*)
from moo_balance_ts
--order by chain, contract_address, investor_address, datetime
;




