explain analyze
select 
    chain, contract_address, investor_address,
    datetime,
    balance_diff,
    sum(balance_diff) over (
        partition by chain, contract_address, investor_address
        order by time_bucket_gapfill('1w', datetime)
    ) as balance
from (
    select chain, contract_address, investor_address,
        time_bucket_gapfill('1w', datetime) as datetime,
        sum(balance_diff) as balance_diff
    from beefy_derived.erc20_balance_diff_4h_ts
    where datetime between '2021-11-19 08:00:00+00' and '2022-07-01 20:00:00+00'
        and contract_address = '\x2425d707a5c63ff5de83eb78f63e06c3f6eeaa1c'
    group by 1,2,3,4
) as t
where balance_diff != 0