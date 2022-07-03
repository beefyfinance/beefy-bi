
CREATE MATERIALIZED VIEW IF NOT EXISTS beefy_derived.moo_balance_diff_4h_ts WITH (timescaledb.continuous) as
    SELECT chain, contract_address, from_address as investor_address, 
        time_bucket('4h', datetime) as datetime,
        sum(value * -1) as moo_balance_diff
    FROM beefy_raw.erc20_transfer_ts
    group by 1,2,3,4
    UNION ALL
    SELECT chain, contract_address, to_address as investor_address, 
        time_bucket('4h', datetime) as datetime, 
        sum(value * 1) as moo_balance_diff
    FROM beefy_raw.erc20_transfer_ts
    group by 1,2,3,4
;