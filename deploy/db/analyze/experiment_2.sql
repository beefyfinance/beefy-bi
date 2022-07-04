explain analyze
with
balance_ts as (
    SELECT chain, contract_address, investor_address, datetime,
        balance_diff, trx_count, deposit_diff, withdraw_diff, deposit_count, withdraw_count,
        sum(balance_diff) over (
            partition by chain, contract_address, investor_address
            order by datetime asc
        ) as balance
    FROM beefy_derived.erc20_balance_diff_4h_ts
    where chain = 'optimism'
),
-- create a balance snapshot every now and then to allow for analysis with filters on date
balance_snapshots_full_hist_ts as (
    select chain, contract_address, investor_address, 
        time_bucket_gapfill('3d', datetime) as datetime,
        locf((array_agg(balance order by datetime desc))[1]) as balance_snapshot,
        lag(locf((array_agg(balance order by datetime desc))[1])) over (
            partition by chain, contract_address, investor_address
            order by time_bucket_gapfill('3d', datetime)
        ) as prev_balance_snapshot
    from balance_ts
    where datetime between now() - interval '3 years' and now()
    group by 1,2,3,4
),
balance_snapshots_ts as (
    select chain, contract_address, investor_address, 
    -- assign the snapshot to the end of the last period
    datetime + interval '3 days' as datetime, 
    balance_snapshot
    from balance_snapshots_full_hist_ts
    where 
        -- remove balances before the first investment
        balance_snapshot is not null 
        -- remove rows after full exit of this user
        and not (prev_balance_snapshot = 0 and balance_snapshot = 0)
    --order by chain, contract_address, investor_address, datetime
),
balance_diff_with_snaps_ts as (
    -- now that we have balance snapshots, we can intertwine them with diffs to get a diff history with some snapshots
    select 
        coalesce(b.chain, bs.chain) as chain, 
        coalesce(b.contract_address, bs.contract_address) as contract_address, 
        coalesce(b.investor_address, bs.investor_address) as investor_address, 
        coalesce(b.datetime, bs.datetime) as datetime, 
        coalesce(b.balance, bs.balance_snapshot) as balance,
        coalesce(b.balance_diff, 0) as balance_diff, 
        coalesce(b.deposit_diff, 0) as deposit_diff,
        coalesce(b.withdraw_diff, 0) as withdraw_diff,
        coalesce(b.trx_count, 0) as trx_count,
        coalesce(b.deposit_count, 0) as deposit_count,
        coalesce(b.withdraw_count, 0) as withdraw_count
    from balance_snapshots_ts bs
    full outer join balance_ts b on 
        b.chain = bs.chain 
        and b.contract_address = bs.contract_address 
        and b.investor_address = bs.investor_address
        and b.datetime = bs.datetime
)
select 
    bt.chain, /*bt.contract_address,*/ vpt.vault_id, bt.investor_address, bt.datetime, 
    bt.balance as token_balance, bt.balance_diff, bt.deposit_diff, bt.withdraw_diff, bt.trx_count, bt.deposit_count, bt.withdraw_count, --vpt.avg_ppfs, vpt.avg_want_usd_value,
    (
        (bt.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
    ) * vpt.avg_want_usd_value as balance_usd_value
from balance_diff_with_snaps_ts as bt
join beefy_derived.vault_ppfs_and_price_4h_ts as vpt 
    on vpt.chain = bt.chain
    and vpt.contract_address = bt.contract_address
    and vpt.datetime = bt.datetime
order by chain, vpt.vault_id, investor_address, datetime

;