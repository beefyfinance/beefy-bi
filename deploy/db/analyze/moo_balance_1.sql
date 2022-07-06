with
balance_4h_ts as (
    select * 
    from (
        select chain, contract_address,
            time_bucket_gapfill('4h', datetime) as datetime,
            balance_diff as balance_diff,
            locf(coalesce(balance, 0)) as balance
        from data_derived.erc20_investor_balance_4h_ts
        where datetime between now() - interval '300 days' and now()
            and investor_address = evm_address_to_bytea('0x18913656c387613f6D0a1bFF7365C335C0069e69')
        group by 1,2,3
    ) as t
    where balance is not null and balance != 0
)
select 
    b.datetime as time,
    sum(
        (
          (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
        )
        * vpt.avg_want_usd_value
    ) as value
from balance_4h_ts b
left join data_derived.vault_ppfs_and_price_4h_ts vpt 
    on b.chain = vpt.chain 
    and b.contract_address = vpt.contract_address
    and b.datetime = vpt.datetime
where vpt.vault_id is not null
group by 1
order by 1





select *
from (
        select chain, contract_address,
            time_bucket_gapfill('4h', datetime) as datetime,
            sum(balance_diff) as balance_diff,
            locf(avg(balance)) as balance
        from data_derived.erc20_investor_balance_4h_ts
        where datetime between now() - interval '300 days' and now()
            and investor_address = evm_address_to_bytea('0x18913656c387613f6D0a1bFF7365C335C0069e69')
        group by 1,2,3
) as t
where balance is not null;


with erc20_investor_balance_4h_ts as (
      select chain, contract_address, investor_address, datetime, balance_diff,
          sum(balance_diff) over (
              partition by chain, contract_address, investor_address
              order by datetime
          ) as balance
      from data_derived.erc20_balance_diff_4h_ts
      where balance_diff != 0
      --order by 1,2,3,4
)


DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT chain, contract_address, min(datetime) as first_datetime, max(datetime) as last_datetime 
        FROM data_derived.erc20_balance_diff_4h_ts
        GROUP BY 1,2
        --limit 10
    LOOP
        EXECUTE '
            INSERT INTO data_derived.erc20_investor_balance_4h_ts (
                chain, contract_address, investor_address,
                datetime,
                balance_diff, balance
            ) 
            select 
                chain, contract_address, investor_address,
                datetime,
                balance_diff,
                sum(balance_diff) over (
                    partition by chain, contract_address, investor_address
                    order by time_bucket_gapfill(''1w'', datetime)
                ) as balance
            from (
                select chain, contract_address, investor_address,
                    time_bucket_gapfill(''1w'', datetime) as datetime,
                    sum(balance_diff) as balance_diff
                from data_derived.erc20_balance_diff_4h_ts
                where datetime between '''||r.first_datetime||''' and '''||r.last_datetime||'''
                    and contract_address = '''||r.contract_address||'''
                group by 1,2,3,4
            ) as t
            where balance_diff != 0
        ';
    END LOOP;
END$$;



select chain, contract_address, investor_address,
    time_bucket_gapfill('4h', datetime) as datetime,
    sum(balance_diff) as balance_diff,
    sum(sum(balance_diff)) over (
        partition by chain, contract_address, investor_address
        order by time_bucket_gapfill('4h', datetime)
    ) as balance
from data_derived.erc20_balance_diff_4h_ts
where $__timeFilter(datetime)
    and investor_address = evm_address_to_bytea('$investor_address')
group by 1,2,3