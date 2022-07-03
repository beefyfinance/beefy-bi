
with 
contract_balance_4h_ts_full as (
    select 
        chain, contract_address, 
        time_bucket_gapfill('4h', datetime) as datetime,
        sum(balance) as balance,
        count(distinct investor_address) as investor_count
    from beefy_derived.erc20_investor_balance_4h_ts
    where investor_address != evm_address_to_bytea('0x0000000000000000000000000000000000000000')
        and datetime between now() - interval '30 day' and now()
    group by 1,2,3
), 
contract_balance_4h_ts as (
    select *
    from contract_balance_4h_ts_full as t
    where balance is not null and balance != 0
)
select 
    b.datetime, 
    vpt.vault_id,
    investor_count,
    (
        (
            (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
        )
        * vpt.avg_want_usd_value
    ) as total_tvl
from contract_balance_4h_ts b
left join beefy_derived.vault_ppfs_and_price_4h_ts vpt 
    on b.chain = vpt.chain 
    and b.contract_address = vpt.contract_address
    and b.datetime = vpt.datetime
where vault_id is not null
order by vault_id, datetime
;




select chain, contract_address, investor_address, 
    time_bucket_gapfill('4h', datetime, now(), now() - interval '30 day') as datetime,
    balance_diff, locf(balance)
from beefy_derived.erc20_investor_balance_4h_ts
where 
    datetime between now() - interval '30 day' and now()
    and contract_address = evm_address_to_bytea('0x6bBdC5cacB4e72884432E3d63745cc8e7A4392Ca')
    and investor_address != evm_address_to_bytea('0x0000000000000000000000000000000000000000')
order by 1,2,3,4
;


    select 
        chain, contract_address, 
        time_bucket_gapfill('4h', datetime) as datetime,
        sum(balance) as balance,
        count(distinct investor_address) as investor_count
    from beefy_derived.erc20_investor_balance_4h_ts
    where 
        contract_address = evm_address_to_bytea('0x6bBdC5cacB4e72884432E3d63745cc8e7A4392Ca')
        and investor_address != evm_address_to_bytea('0x0000000000000000000000000000000000000000')
        and datetime between now() - interval '30 day' and now()
    group by 1,2,3
    ;


select *
from beefy_derived.erc20_investor_balance_4h_ts
limit 100;