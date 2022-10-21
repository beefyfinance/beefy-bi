
select * 
from price_ts
where price_feed_id = 216334 
and block_number = 29768611


create table tmp_duplicate_ppfs as (
select price_feed_id, block_number, array_agg(distinct datetime) as datetimes, array_agg(distinct price) as prices, jsonb_agg(price_data) as price_datas
from price_ts 
group by price_feed_id, block_number
having count(distinct datetime) > 1
);


select *
from investment_balance_ts
where product_id = 3
and block_number = 29768611;






          3 | beefy:vault:arbitrum:stargate-arb-usdc                       
          | arbitrum  |          216334 |          216361 |
           {"type": "beefy:vault", "vault": {"id": "stargate-arb-usdc", "eol": false, "chain": "arbitrum", "assets": ["sarUSDC"], "protocol": "unknown", "token_name": "mooStargateUSDC", "is_gov_vault": false, "want_address": "0x892785f33CdeE22A30AEF750F285E18c18040c3e", "want_decimals": 6, "token_decimals": 18, "contract_address": "0xD496eA90a454eA49e30A8fD9E053C186D4FC897D", "protocol_product": "sarUSDC", "want_price_feed_key": "sarUSDC"}}

            product:investment:3 | {"type": "product:investment", "chain": "arbitrum", "ranges": {"toRetry": [], "coveredRanges": [{"to": 31125271, "from": 26492698}], "lastImportDate": "2022-10-19T23:51:00.211Z"}, "productId": 3, "contractCreationDate": "2022-09-22T12:50:20.000Z", "chainLatestBlockNumber": 31125276, "contractCreatedAtBlock": 25961698}





create table tmp_duplicate_investment as (select 
    product_id,
    block_number,
    investor_id,
    array_agg(distinct datetime) as datetimes, 
    array_agg(distinct balance::varchar) as balances,
    jsonb_agg(investment_data) as investment_datas
from investment_balance_ts 
group by product_id, block_number, investor_id
having count(distinct datetime) > 1)
;
select (select count(*) from tmp_duplicate_investment), count(*) from (select 
          product_id,
          block_number,
          investor_id,
          array_agg(distinct datetime) as datetimes, 
          array_agg(distinct balance::varchar) as balances,
          jsonb_agg(investment_data) as investment_datas
        from investment_balance_ts 
        group by product_id, block_number, investor_id
        having count(distinct datetime) > 1)
as t ;


select product_id, block_number, investor_id, jsonb_pretty(
    jsonb_build_object(
        'datetimes', datetimes,
        'balances', balances,
        'investment_datas', investment_datas
    )
) from tmp_duplicate_investment_2 where (product_id, block_number, investor_id) not in ( select product_id, block_number, investor_id from tmp_duplicate_investment);




update import_state set import_data = 
jsonb_set(
jsonb_set(import_data, '{ranges,coveredRanges}', '[]'::jsonb)
, '{ranges,toRetry}', '[]'::jsonb)
where import_key in (select 'price:feed:'|| price_feed_1_id from product);

update import_state set import_data = 
jsonb_set(
jsonb_set(import_data, '{ranges,coveredRanges}', '[]'::jsonb)
, '{ranges,toRetry}', '[]'::jsonb)
where import_key like 'product:investment:%';

delete from price_ts where price_feed_id in (select price_feed_1_id from product);

truncate table investment_balance_ts;



update import_state set import_data = 
jsonb_set(
jsonb_set(import_data, '{ranges,coveredRanges}', '[]'::jsonb)
, '{ranges,toRetry}', '[]'::jsonb)
where import_key in (select 'price:feed:'|| price_feed_1_id from product);







delete from price_ts where price_feed_id in (select price_feed_1_id from product);
vacuum full analyze price_ts;


