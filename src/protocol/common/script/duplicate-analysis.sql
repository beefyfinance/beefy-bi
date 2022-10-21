
-- find out the block numbers of this chain
select 
    time_bucket_gapfill('15 minutes', datetime) as datetime,
    last(block_number, datetime) as block_number,
    interpolate(last(block_number, datetime)) as interpolated_block_number
from investment_balance_ts
where 
    datetime between ('2022-09-22T12:50:20.000Z'::timestamptz - '15 minutes'::interval) and (now() - '15 minutes'::interval)
    and product_id in (
        select product_id
        from product
        where chain = 'bsc'
    )
group by 1;

