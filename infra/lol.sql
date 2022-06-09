with 
scoped_transfers as (
        SELECT *
        from erc20_transfer
    WHERE 
        chain = 'fantom' 
        --AND contract_address = '0x41D44B276904561Ac51855159516FD4cB2c90968'
        AND contract_address = '0x95EA2284111960c748edF4795cb3530e5E423b8c'
),
balance_delta as materialized (
    (
        SELECT 
            (time at time zone 'utc')::timestamp without time zone as time,
            from_address as account_address,
            (cast(value as DECIMAL(78,18)) * -1)/POWER(10.0::DECIMAL(78,18),18.0) as value
        FROM scoped_transfers
        WHERE 
            from_address != '0x0000000000000000000000000000000000000000' /* mint/burn */
            and from_address is not null
    )
    UNION ALL
    (
        SELECT 
            (time at time zone 'utc')::timestamp without time zone as time,
            to_address as account_address,
            (cast(value as DECIMAL(78,18)))/POWER(10.0::DECIMAL(78,18),18.0) as value
        FROM scoped_transfers
        WHERE 
            to_address != '0x0000000000000000000000000000000000000000' /* mint/burn */
            and to_address is not null
    )
 ), 
 accounts as (
    select distinct account_address
    from balance_delta
 ),
 time_serie_bounds as (
     select 
        -- we attribute the data to the upper bound of the range
        date_trunc('minute', time::timestamp without time zone at time zone 'utc') + interval '15 minutes' as time,
        tsrange(
            date_trunc('minute', time::timestamp without time zone),
            date_trunc('minute', time::timestamp without time zone) + interval '15 minutes'
        ) as time_range
     from generate_series(
            (select date_trunc('hour', min(time at time zone 'utc')) from scoped_transfers),
            (select date_trunc('hour', now()) + interval '1 hour'),
            interval '15 minutes'
        ) as time_series(time)
 ), 
account_delta_ts as (
    select 
        time_serie_bounds.time,
        time_serie_bounds.time_range,
        account_address, 
        sum(coalesce(value, 0.0::DECIMAL(78,18))) as balance_delta
    from time_serie_bounds
        join balance_delta on time_range @> balance_delta.time
    group by 1,2,3
 ),
 account_balance_ts as (
    select 
        time,
        time_range,
        account_address,
        sum(coalesce(balance_delta, 0.0::DECIMAL(78,18))) over w as account_balance
    from account_delta_ts
    window w as (partition by account_address order by time asc rows between unbounded preceding and current row)
 )
 select * 
 from accounts, time_serie_bounds;
 /*
 select * 
 from balance_delta
 order by time asc
limit 10*/
 ;