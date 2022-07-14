create table tmp_3d (
    datetime timestamptz,
    balance_usd_ranges_counts integer[]
);

set jit_above_cost = -1;
DEALLOCATE stmt;
PREPARE stmt AS 
    INSERT INTO tmp_3d (datetime, balance_usd_ranges_counts) (
        with balance_3d_ts as (
            select * from (
                SELECT
                chain, contract_address, owner_address,
                time_bucket_gapfill('3d', datetime) AS datetime,
                locf(last(balance_after::numeric, datetime)) as balance
                FROM data_derived.erc20_owner_balance_diff_4h_ts
                WHERE
                --$__timeFilter(datetime)
                datetime between now() - interval '1 year' and now()
                group by 1,2,3,4
            ) as t
            where t.balance is not null -- remove past history
                and t.balance > 0
        ),
        price_3d_ts as (
            select 
                chain, want_decimals, contract_address,
                time_bucket_gapfill('3d', datetime) as datetime,
                avg(avg_ppfs) as avg_ppfs, avg(avg_want_usd_value) as avg_want_usd_value
            from data_derived.vault_ppfs_and_price_4h_ts
            WHERE
                --$__timeFilter(datetime)
                datetime between now() - interval '1 year' and now()
            group by 1,2,3,4
        ),
        owner_usd_balance_by_vault_ts as (
            select b.chain, b.contract_address, b.owner_address, b.datetime,
                (
                    (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
                )
                * vpt.avg_want_usd_value as balance_usd_value
            from balance_3d_ts b 
                left join price_3d_ts vpt 
                    on b.chain = vpt.chain 
                    and b.contract_address = vpt.contract_address 
                    and b.datetime = vpt.datetime
            where vpt.avg_ppfs is not null and vpt.avg_want_usd_value is not null
        ),
        owner_usd_balance_total_ts as (
            select datetime, owner_address, sum(balance_usd_value) as balance_usd_value
            from owner_usd_balance_by_vault_ts
            group by 1,2
        ),
        buckets_ts as (
            select datetime,
                intarray_sum_elements_agg(
                    case 
                        when balance_usd_value is null then null
                        when balance_usd_value < 0.0        then ARRAY[1,0,0,0,0,0,0,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value = 0.0        then ARRAY[0,1,0,0,0,0,0,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 10.0       then ARRAY[0,0,1,0,0,0,0,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 100.0      then ARRAY[0,0,0,1,0,0,0,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 1000.0     then ARRAY[0,0,0,0,1,0,0,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 5000.0     then ARRAY[0,0,0,0,0,1,0,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 10000.0    then ARRAY[0,0,0,0,0,0,1,0,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 25000.0    then ARRAY[0,0,0,0,0,0,0,1,0,0,0,0,0,0]::int[]
                        when balance_usd_value < 50000.0    then ARRAY[0,0,0,0,0,0,0,0,1,0,0,0,0,0]::int[]
                        when balance_usd_value < 100000.0   then ARRAY[0,0,0,0,0,0,0,0,0,1,0,0,0,0]::int[]
                        when balance_usd_value < 500000.0   then ARRAY[0,0,0,0,0,0,0,0,0,0,1,0,0,0]::int[]
                        when balance_usd_value < 1000000.0  then ARRAY[0,0,0,0,0,0,0,0,0,0,0,1,0,0]::int[]
                        when balance_usd_value < 10000000.0 then ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,1,0]::int[]
                                                            else ARRAY[0,0,0,0,0,0,0,0,0,0,0,0,0,1]::int[]
                    end
                ) as balance_usd_ranges_counts
            from owner_usd_balance_total_ts
            group by 1
        )
        select * from buckets_ts
    )
;
execute stmt;



select datetime as time,
    locf(sum(balance_usd_ranges_counts[1])) as "01) < $0",
    locf(sum(balance_usd_ranges_counts[2])) as "02) = $0",
    sum(balance_usd_ranges_counts[3]) as "03) ($0 ; $10)",
    sum(balance_usd_ranges_counts[4]) as "04) [$10 ; $100)",
    sum(balance_usd_ranges_counts[5]) as "05) [$100 ; $1k)",
    sum(balance_usd_ranges_counts[6]) as "06) [$1k ; $5k)",
    sum(balance_usd_ranges_counts[7]) as "07) [$5k ; $10k)",
    sum(balance_usd_ranges_counts[8]) as "08) [$10k ; $25k)",
    sum(balance_usd_ranges_counts[9]) as "09) [$25k ; $50k)",
    sum(balance_usd_ranges_counts[10]) as "10) [$50k ; $100k)",
    sum(balance_usd_ranges_counts[11]) as "11) [$100k ; $500k)",
    sum(balance_usd_ranges_counts[12]) as "12) [$500k ; $1m)",
    sum(balance_usd_ranges_counts[13]) as "13) [$1m ; $10m)",
    sum(balance_usd_ranges_counts[14]) as "14) +$10m"
from tmp_3d
where $__timeFilter(datetime)
group by 1
order by 1;






with balance_3d_ts as (
    select * from (
        SELECT
        chain, contract_address, owner_address,
        time_bucket_gapfill('3d', datetime) AS datetime,
        locf(last(balance_after::numeric, datetime)) as balance
        FROM data_derived.erc20_owner_balance_diff_4h_ts
        WHERE
        --$__timeFilter(datetime)
        datetime between now() - interval '1 year' and now()
        and chain in ('bsc')
        group by 1,2,3,4
    ) as t
    where t.balance is not null -- remove past history
        and t.balance > 0
),
price_3d_ts as (
    select 
        chain, vault_id, want_decimals, contract_address,
        time_bucket_gapfill('3d', datetime) as datetime,
        avg(avg_ppfs) as avg_ppfs, avg(avg_want_usd_value) as avg_want_usd_value
    from data_derived.vault_ppfs_and_price_4h_ts
    WHERE
        --$__timeFilter(datetime)
        datetime between now() - interval '1 year' and now()
        and chain in ('bsc')
    group by 1,2,3,4,5
),
owner_usd_balance_by_vault_ts as (
    select b.chain, b.contract_address, vpt.vault_id, b.owner_address, b.datetime, b.balance, vpt.avg_ppfs, vpt.avg_want_usd_value,
        (
            (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
        )
        * vpt.avg_want_usd_value as balance_usd_value
    from balance_3d_ts b 
        left join price_3d_ts vpt 
            on b.chain = vpt.chain 
            and b.contract_address = vpt.contract_address 
            and b.datetime = vpt.datetime
    where 
        vpt.avg_ppfs is not null 
        and vpt.avg_want_usd_value is not null
        and vpt.datetime = '2022-06-30 00:00:00'
        and b.datetime = '2022-06-30 00:00:00'
)
select chain, vault_id, format_evm_address(contract_address) as contract_address,
 datetime, format_evm_address(owner_address) as owner_address, balance, avg_ppfs, avg_want_usd_value, balance_usd_value
from owner_usd_balance_by_vault_ts
where balance_usd_value > 0 and balance_usd_value < 10.0
limit 100
;

 chain |      vault_id      |             contract_address               |        datetime        |             owner_address                  |       balance        |      avg_ppfs       | avg_want_usd_value  |   balance_usd_value   
-------+--------------------+--------------------------------------------+------------------------+--------------------------------------------+----------------------+---------------------+---------------------+-----------------------
 bsc   | annex-ann-busd-eol | 0x6d56c98df063ef0cb7431aaf87ba6c2f2fe24611 | 2022-06-30 00:00:00+00 | 0xf04523fa1f120591bff3caff1d7d610399dbc114 |    86583605094179908 | 1870089411215024830 |  0.3964815556194385 |   0.06419792994064123
 bsc   | auto-usdt-busd     | 0x17720f863da01bc9e266e4ee872e3c98fa1feaa8 | 2022-06-30 00:00:00+00 | 0x7b33823397b23d2a2fe1ffb63ef11af55e178d01 |   465380808489320075 | 1100497916692888388 |  2.0202998266657946 |    1.0346977890367688
 bsc   | auto-usdt-busd     | 0x17720f863da01bc9e266e4ee872e3c98fa1feaa8 | 2022-06-30 00:00:00+00 | 0xf04523fa1f120591bff3caff1d7d610399dbc114 |    92245419572096130 | 1100497916692888388 |  2.0202998266657946 |      0.20509253913982
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0xe59ddd457384089c3283c5cab2e8eaa03fbd87a4 |       95708750222528 | 1034155129759253439 |   85939.18153330237 |     8.506062098823302
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0xd48ceee14152eaebb570c8bf07e539fcd1c743a5 |       20043886373144 | 1034155129759253439 |   85939.18153330237 |     1.781389285674633
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x925083dd8426b5b193a60be7a45e9d3482a108fa |         321821892772 | 1034155129759253439 |   85939.18153330237 |  0.028601742247336814
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x9176fa969181058a8d477436342113d7299afc06 |         116600570668 | 1034155129759253439 |   85939.18153330237 |  0.010362811054937266
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x58da44d40f060928977ecb68c2ff64d48e5c17d9 |       32152685114154 | 1034155129759253439 |   85939.18153330237 |     2.857552058605093
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x51e1db8638b493bc54e86d9856a37a45469f8310 |       44256073864453 | 1034155129759253439 |   85939.18153330237 |    3.9332340216113306
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x3f999ed5694beaa260a1c80447a22f4ae1ba8dac |        1416750980924 | 1034155129759253439 |   85939.18153330237 |    0.1259129577419955
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x250e8d3efa143c4b97a2df9f08c08650d2f00fc3 |       18108113376701 | 1034155129759253439 |   85939.18153330237 |    1.6093485341374376
 bsc   | baby-aot-usdt-eol  | 0xc9215f674876da17a671f22c2083e200ed78d0c8 | 2022-06-30 00:00:00+00 | 0x23194dad026058c672115609c55132e4547f1e51 |       52201511211076 | 1034155129759253439 |   85939.18153330237 |    4.6393803594910725
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x9c6229595103abc09773d36a16861e721a432251 |   830922175248397314 | 1115916875823291321 |  10.163110037692284 |     9.423642942603655
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x8df6dbc2861d3c0796cc94df386c01db405945f6 |   479904413455214294 | 1115916875823291321 |  10.163110037692284 |      5.44268521613306
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x83d457baadea5797c9326c20d7a088a66fa7d328 |   481991276274690738 | 1115916875823291321 |  10.163110037692284 |     5.466352715529213
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x769ddb875d7a0dda94343c4f2319c6ab6e326313 |   167196154042488693 | 1115916875823291321 |  10.163110037692284 |    1.8962026817998452
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x6390a821ebf5dd20ede7d3362ca4c29d936efe89 |   616525350770293652 | 1115916875823291321 |  10.163110037692284 |     6.992128677978649
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x5a97b5a80a932cd1f98a268f5a8ea40f3edb273a |   214196497053305085 | 1115916875823291321 |  10.163110037692284 |      2.42924231403908
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x50f8496f7d9c3ce21337303f710e6e3595de8f18 |   864616326040569081 | 1115916875823291321 |  10.163110037692284 |     9.805774573914075
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x460e8638bbafdc5e855fb71e2b0fed6acdd2e451 |   234557165205501277 | 1115916875823291321 |  10.163110037692284 |    2.6601564386763004
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x4057b5327fb3b75f5684a189936d61612749aaae |        5620149701981 | 1115916875823291321 |  10.163110037692284 | 6.373916312874504e-05
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x2c36ae762d08f656a71a06463ece76a8f46c1610 |   631563401580194584 | 1115916875823291321 |  10.163110037692284 |     7.162678009319907
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0x1ff5202086e0111f8a8b37863bc0178216770cde |    87979404425117423 | 1115916875823291321 |  10.163110037692284 |    0.9977907899225122
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0xfe0cc236aee4eac87bf2b96418a531ea1614bafc |   800881101051190472 | 1115916875823291321 |  10.163110037692284 |     9.082941532435955
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0xf91bf7a177a86483cd41a2780026023ccdcff9d9 |   454941094291895598 | 1115916875823291321 |  10.163110037692284 |     5.159571570276821
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0xe38e8843eeb8dd516131a96b669431b5c654e6c0 |      531442335764256 | 1115916875823291321 |  10.163110037692284 |  0.006027186379191884
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0xd378d11e5587dbc55afb9f946bfa453d6bf683df |   730221534736364375 | 1115916875823291321 |  10.163110037692284 |     8.281578248045225
 bsc   | baby-avax-usdt     | 0xc5bb189e9fb1fb0ce81a6f5b16db4b6d30bc6dab | 2022-06-30 00:00:00+00 | 0xbfb5e3aed5bf181c0bab57668702325d40154a57 |   746602076329555803 | 1115916875823291321 |  10.163110037692284 |      8.46735301706563
 bsc   | baby-avax-wbnb     | 0xebcbac449d17f73aec163302d66ab1e70088583d | 2022-06-30 00:00:00+00 | 0xc75e1b127e288f1a33606a52ab5c91bbe64eaafe |    17865068662875299 | 1008066042331562279 |  120.12110513385682 |     2.163281290385979
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xd1f4cd50a4b870f20ea9c93a5b811822c124e66f |  6897583018596348334 | 1510259155427594941 | 0.10136864671114852 |    1.0559711719478093
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xd188d1c76ec9c215d6542b4f9461fdad716f6f83 |  7006164064679710923 | 1510259155427594941 | 0.10136864671114852 |    1.0725941620843311
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xd06b2d771c97dc78d58df56bbf56ad16da9e4383 |  4853366977382323298 | 1510259155427594941 | 0.10136864671114852 |    0.7430161552505887
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xcd09db83862cffda9b5b1b7a4dcd72e8c1de0b11 | 13125415926675423561 | 1510259155427594941 | 0.10136864671114852 |    2.0094083392727904
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xcaf66dc8e0a9ab24616a4bf27611854662b56aa5 |  1178901416511788918 | 1510259155427594941 | 0.10136864671114852 |    0.1804814682256944
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xca6fa4964e7e50597e3d19fdbe49aecc8f0bc784 |  1249146086929902820 | 1510259155427594941 | 0.10136864671114852 |   0.19123543040991445
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc7af9525f40eb74b81ee25f92a2bb4789d783b32 |   529810646540747497 | 1510259155427594941 | 0.10136864671114852 |   0.08111026251220246
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc73471c73831ace92383defb346fbb9c028f94c8 |   141748733282209035 | 1510259155427594941 | 0.10136864671114852 |  0.021700728443945858
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc614ac2674a08828a794bc182c20653cf5c3b673 |  1264086860028619666 | 1510259155427594941 | 0.10136864671114852 |   0.19352275709178582
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc60b24e95f5683b3371032b3fef9c4a9e038cdc3 |  3298029451334648890 | 1510259155427594941 | 0.10136864671114852 |    0.5049049812745785
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc35209f9e2e47ec71e6651d2379dfa848637622f | 11479215095230487176 | 1510259155427594941 | 0.10136864671114852 |    1.7573866359376245
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc2bab278bf2bb57d4d36544ec58432de414ec970 |  2693932549112611548 | 1510259155427594941 | 0.10136864671114852 |    0.4124220184614307
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc054b7f3f50fd98cc6560ee9f66da5c0d7b192d2 | 39805617950203127826 | 1510259155427594941 | 0.10136864671114852 |     6.093958553837972
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc0442a99c8b0e656986f20c9519cdbcbd52e8c5f | 23014653388470305590 | 1510259155427594941 | 0.10136864671114852 |    3.5233806458108003
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xc022c50c295ca25dba3579630a6fddbd9359e91e |  2236328621919936284 | 1510259155427594941 | 0.10136864671114852 |    0.3423660939465992
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xbebd699f9e724fa864d35b51e51b2db05bc1a03d |   170710123047268115 | 1510259155427594941 | 0.10136864671114852 |   0.02613451236637123
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xbdfa9adbaf29760acb98d12e1045bea27e80a9da | 26316690651190961951 | 1510259155427594941 | 0.10136864671114852 |       4.0288991946604
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xbaca2a141ba0349c87c29bc6932dbd17e354029b |    43606375671122158 | 1510259155427594941 | 0.10136864671114852 | 0.0066758276772726464
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb933c2afd7116aac86ad3835c9df86312326aa06 |      278079884283212 | 1510259155427594941 | 0.10136864671114852 |   4.2572063360451e-05
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb8989cdca809c201c6a15c400585da728ed5364f | 27493849159492319550 | 1510259155427594941 | 0.10136864671114852 |      4.20911383596707
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb835f2736a2bafafb8e4a250fe130dc08b74006e |  7761197175562662957 | 1510259155427594941 | 0.10136864671114852 |    1.1881843908367673
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb6a304628e91689a11235661f0c31a8d4395f546 | 49722221331466234958 | 1510259155427594941 | 0.10136864671114852 |     7.612120389081092
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb43fe897eade03af5b99886b07ec99d18414df77 |  4175592910206271831 | 1510259155427594941 | 0.10136864671114852 |     0.639253739618602
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb34e7fb6ee8d42c6093b239fda0432365c81fa4b |  2168396959117769768 | 1510259155427594941 | 0.10136864671114852 |   0.33196623686794313
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb2829302dbc1534f66f7867ae2e42344fb47b572 | 12287659045754676219 | 1510259155427594941 | 0.10136864671114852 |    1.8811536864519174
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb19b8851eafd17f7f241f5553a81e5bcf693531a |  2114262043667777807 | 1510259155427594941 | 0.10136864671114852 |   0.32367856422132135
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xb0d98e75cbec87b26f6e4322b97d76f2a5000e1c |  7047101249186015693 | 1510259155427594941 | 0.10136864671114852 |    1.0788613554740762
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xaff241de8362350a5904a016a5c6974fca5f1504 |   534271005031131774 | 1510259155427594941 | 0.10136864671114852 |   0.08179311184793354
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xaf3da21f16736f4582cf09b58307f78aaac14234 | 19279690541041030394 | 1510259155427594941 | 0.10136864671114852 |     2.951584252125056
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xae24727e72277ef7ac1b2828cd24261092ddfb45 |  3778935187086572977 | 1510259155427594941 | 0.10136864671114852 |    0.5785282478607521
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xabdbd963b429c49b93499b8a9e0164e90eb6e5cf | 22752357183567221652 | 1510259155427594941 | 0.10136864671114852 |    3.4832249521218333
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa9eaef87c01b4c46a691862c7ba94401394b8b9c |  5832176294812970306 | 1510259155427594941 | 0.10136864671114852 |     0.892864938404635
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa9bbfb1263d13ef932b345dd5ce18e27df7c45f8 | 17531183491393972977 | 1510259155427594941 | 0.10136864671114852 |     2.683900190418678
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa9289768711f7fe1631e419b90d932a48f0db454 |   144088229165379404 | 1510259155427594941 | 0.10136864671114852 |  0.022058888715864013
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa6aa00d67f170c915feb57fcd49cc1c55432f008 |  3082805670247864865 | 1510259155427594941 | 0.10136864671114852 |    0.4719557427177515
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa54a0748cdc670076fa9e37d99dd95b9561638cb | 52956903591869108601 | 1510259155427594941 | 0.10136864671114852 |     8.107327363493342
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa515521f449a9d2ec1830d383b0683fc0a655422 |   119149659025972336 | 1510259155427594941 | 0.10136864671114852 |  0.018240970023792748
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa376dbf6173efe064131ad6cc75db2f1bad2fc8e |  2349119624468186635 | 1510259155427594941 | 0.10136864671114852 |   0.35963359863989997
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa34780a078326490a5afe3bce022138de0db493e | 18218749370839472702 | 1510259155427594941 | 0.10136864671114852 |    2.7891616632493657
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0xa274f4bb88a0bc689049a02b3738e6584fa4ac9c | 14452646726324838068 | 1510259155427594941 | 0.10136864671114852 |     2.212597986888837
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x9f48deb6962b893de6b0b7052b42637a8a992ae8 | 54777073241456379547 | 1510259155427594941 | 0.10136864671114852 |      8.38598246236443
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x9e62bc0752c5635af48d5f65d7050d6ffd021d76 |  3576765942502286402 | 1510259155427594941 | 0.10136864671114852 |    0.5475775665047028
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x9be0e28896ba3c294f12d003760bf3a31f41cab1 |   753145599134911142 | 1510259155427594941 | 0.10136864671114852 |   0.11530126405461807
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x99c2e4708493b19baa116e26dfa0056f5a69a783 |  1541503470139627107 | 1510259155427594941 | 0.10136864671114852 |   0.23599327786796384
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x9815df5c5669595f5dbe3323ef98a17ab018485c |  1613722542940949947 | 1510259155427594941 | 0.10136864671114852 |   0.24704950709164866
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x97b0af18b34397b2e3ec1dc38c23891e93ff26da | 11871380590894385459 | 1510259155427594941 | 0.10136864671114852 |    1.8174243994465544
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x978aa0e4592e47327bace8d157851e4bb0bd3cc5 |  2062910294084138590 | 1510259155427594941 | 0.10136864671114852 |    0.3158169745828626
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x97565f965e5fb10787bc065db78d9faee3667511 |    90980512809677330 | 1510259155427594941 | 0.10136864671114852 |  0.013928472984961386
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x96b503af0c0df8cad96c7719311ad1104e9b10c5 | 58572720360502079435 | 1510259155427594941 | 0.10136864671114852 |     8.967069188800766
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x952fd0a8deb46e5ecaf1ffee48f3c7a070907903 |  4696969666258425356 | 1510259155427594941 | 0.10136864671114852 |     0.719072833151858
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x951adc46f2a7a172b3b66c696cfdfe8aecd74694 |   631926807788438878 | 1510259155427594941 | 0.10136864671114852 |   0.09674352450800804
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8fe0e5631db7931c6eb68de1dba62a4df90cd6f2 | 10770646834626794717 | 1510259155427594941 | 0.10136864671114852 |    1.6489098471063148
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8f639a45ca705a6cd27a8159772ecceb88e33ab7 |  3054575333950871054 | 1510259155427594941 | 0.10136864671114852 |   0.46763387791037675
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8ee13e3700dd582afc47529eb4176ebd6237cd3b |   321723859048110991 | 1510259155427594941 | 0.10136864671114852 |  0.049253647193033795
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8d339df2eddd662cc27c830fbef7ab21194f7ef4 |  3503304151496243075 | 1510259155427594941 | 0.10136864671114852 |    0.5363310859139083
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8cda68871ad593907fb155294f19e5900fd762e2 | 12545568260674316620 | 1510259155427594941 | 0.10136864671114852 |    1.9206377630046132
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8a9c2947c396b3ec72b1dd6c303058cd14565f96 | 22989830673502241581 | 1510259155427594941 | 0.10136864671114852 |     3.519580463725991
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x89f14f9064b4fea39ac8e09713f0a301d587dff8 |  4607641137567092458 | 1510259155427594941 | 0.10136864671114852 |    0.7053972672505495
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x8593acbfbea52cbd81ce2f3bc2e24ae820ae6752 |   485846202921706607 | 1510259155427594941 | 0.10136864671114852 |   0.07437961716480083
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x856b63349fb6c818ea7cd7305483ae0ef6956f6c | 13445462460981031639 | 1510259155427594941 | 0.10136864671114852 |     2.058405199911853
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x84bef8c6b91b34a30a7260810ce3036b453fa36c |  2329710777761441358 | 1510259155427594941 | 0.10136864671114852 |   0.35666224149235703
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x83c6cfb135d8750c7bad29991cef95c660798791 | 45454951035624161537 | 1510259155427594941 | 0.10136864671114852 |    6.9588314901769905
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x830084525e818346c68ac1a6fcaf4a1cf324021e | 55789589772993072269 | 1510259155427594941 | 0.10136864671114852 |     8.540991581579194
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x81bed01cf4fa97aa89dbbf7c0e9dea7fd599ccd4 |  4396994926343072105 | 1510259155427594941 | 0.10136864671114852 |    0.6731488222615016
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7fdf77e88cbe0815fe6c8f32cc2c24d81678bd07 |  3508376138602303544 | 1510259155427594941 | 0.10136864671114852 |    0.5371075712645088
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7f4c1ae53b95c8e604e82535cba1ae60c9f5b082 |  2350669440511351037 | 1510259155427594941 | 0.10136864671114852 |   0.35987086451390127
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7f2e77502a04d34f57601c18433b398aca273ae3 |  5386059652873358088 | 1510259155427594941 | 0.10136864671114852 |    0.8245676360098231
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7f25eec12603a564676c53d98c58dde35aabf52b | 40026292849243063135 | 1510259155427594941 | 0.10136864671114852 |    6.1277423199964085
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7cf97872e63d1d3d6140d7f2c3868c6bf6869121 | 19376204726544482728 | 1510259155427594941 | 0.10136864671114852 |     2.966359891258488
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7cd3849695a87872dcb6446975292a6c8eb50e31 |  2414358571644241641 | 1510259155427594941 | 0.10136864671114852 |    0.3696212200023985
 bsc   | baby-baby          | 0x87f9a89b51da28ce8653a700d362cda9b9ba7d88 | 2022-06-30 00:00:00+00 | 0x7c9e80caa89f94188250066f78b5f53ff2039ccd | 58387238219915616671 | 1510259155427594941 | 0.10136864671114852 |      8.93867318503504
(100 rows)





with balance_3d_ts as (
    select * from (
        SELECT
        chain, contract_address, owner_address,
        time_bucket_gapfill('3d', datetime) AS datetime,
        locf(last(balance_after::numeric, datetime)) as balance
        FROM data_derived.erc20_owner_balance_diff_4h_ts
        WHERE
        --$__timeFilter(datetime)
        datetime between now() - interval '1 year' and now()
        group by 1,2,3,4
    ) as t
    where t.balance is not null -- remove past history
        and t.balance > 0
),
price_3d_ts as (
    select 
        chain, want_decimals, vault_id, contract_address,
        time_bucket_gapfill('3d', datetime) as datetime,
        avg(avg_ppfs) as avg_ppfs, avg(avg_want_usd_value) as avg_want_usd_value
    from data_derived.vault_ppfs_and_price_4h_ts
    WHERE
        --$__timeFilter(datetime)
        datetime between now() - interval '1 year' and now()
    group by 1,2,3,4,5
),
owner_usd_balance_by_vault_ts as (
    select b.chain, b.contract_address, vault_id, b.owner_address, b.datetime,
        (
            (b.balance::NUMERIC * vpt.avg_ppfs::NUMERIC) / POW(10, 18 + vpt.want_decimals)::NUMERIC
        )
        * vpt.avg_want_usd_value as balance_usd_value
    from balance_3d_ts b 
        left join price_3d_ts vpt 
            on b.chain = vpt.chain 
            and b.contract_address = vpt.contract_address 
            and b.datetime = vpt.datetime
            and vpt.datetime = '2022-06-30 00:00:00'
            and b.datetime = '2022-06-30 00:00:00'
    where vpt.avg_ppfs is not null and vpt.avg_want_usd_value is not null
),
owner_usd_balance_total_ts as (
    select datetime, owner_address, sum(balance_usd_value) as balance_usd_value, array_agg(vault_id) as vault_ids
    from owner_usd_balance_by_vault_ts
    group by 1,2
)
select datetime, format_evm_address(owner_address) as owner_address, balance_usd_value, vault_ids
from owner_usd_balance_total_ts
where balance_usd_value > 0 and balance_usd_value < 10.0
order by random()
limit 100;

        datetime        |               owner_address                |   balance_usd_value   |                                                 vault_ids                                                  
------------------------+--------------------------------------------+-----------------------+------------------------------------------------------------------------------------------------------------
 2022-06-30 00:00:00+00 | 0x71b651d8cf6cb03b6fabf50a4cb6a1498b84938b |   0.19710629240854738 | {2omb-2omb-ftm-eol,charge-static-busd-eol}
 2022-06-30 00:00:00+00 | 0x837bc94233ec053f0f6f221ced07c5eae2b1e97b |     2.306738941004962 | {ape-watch-matic}
 2022-06-30 00:00:00+00 | 0xfaf25e267893c64526cdddfbbd9b823b363c9f43 |    0.0194284144746954 | {bomb-bomb-btcb}
 2022-06-30 00:00:00+00 | 0xa12929926f5989ab396c420d55bacc812ccdefa2 |   0.17780957416927598 | {fuse-fuse}
 2022-06-30 00:00:00+00 | 0x623ea2556177a81756cb7baa7ed8ceb0584920a8 |    1.3087141078881606 | {mdex-bsc-mdx}
 2022-06-30 00:00:00+00 | 0x4cbd7c9199b93b54e55d1887d6492d5a1d39c825 |    0.4015649589249513 | {czf-czf-bnb}
 2022-06-30 00:00:00+00 | 0x8fb36f0ef38b4cda91da93e42a774fe975d3bac0 |     2.308815405221291 | {ripae-pavax-wavax}
 2022-06-30 00:00:00+00 | 0x8879610b84998f8b564949b21effa51c25f92217 |     1.920157847734517 | {joe-wavax-more,joe-joe,blizz-blzz-avax,netswap-nett-metis}
 2022-06-30 00:00:00+00 | 0x446b0491b5193474110dbeb55d9b6f04deff0da2 |   0.01973583745845477 | {joe-joe}
 2022-06-30 00:00:00+00 | 0x9cc28d3c615a7cb4cdc5e8a218afc0b0148b94dd |    0.8052988839358792 | {joe-wavax-more}
 2022-06-30 00:00:00+00 | 0xad8a2b3464a8e947615f6ac20d01d9c5e77cf70b |    0.4906555673442046 | {joe-wavax-spell-eol,banana-sushi-eth}
 2022-06-30 00:00:00+00 | 0x4bc9c1c08b2ec0c611c7867617f19636f4578d3a | 0.0036828410419548693 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x6b81844b5ad23efe48a8569276c295261893cb2d |      5.43566682114234 | {based-based-tomb}
 2022-06-30 00:00:00+00 | 0x8d26af7f94799040156b0b5c57796f7dfe7471dd |     9.117427720013124 | {fuse-fuse}
 2022-06-30 00:00:00+00 | 0xa1d120ebdb1bd208356c0148c1e47c0d448ea1a4 | 0.0038492540275096706 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0xf64d63a924b005f237eba17f47793186d4b373e8 |    1.7805795132281426 | {boo-boo}
 2022-06-30 00:00:00+00 | 0xb6c0b21924ac40d8a5ff6c52d3bd1cf5fea29cf1 |    0.4556543268815969 | {spirit-binspirit-spirit}
 2022-06-30 00:00:00+00 | 0xef1bd8a377255c5c82773ac09fbcc033529a282b |  0.018972454371799393 | {geist-geist-ftm}
 2022-06-30 00:00:00+00 | 0x074cb898ffe7cd71947b96738b626610b543237a |   0.21240050142998615 | {bifi-maxi-eol}
 2022-06-30 00:00:00+00 | 0x1d433b776f4607c273ce9298d3f3f4779d6594b0 | 0.0018474377692319446 | {curve-arb-tricrypto}
 2022-06-30 00:00:00+00 | 0x1993ada5e438516ed2a82aac4f048cea5a145af0 |     4.441620303683003 | {vvs-cro-atom}
 2022-06-30 00:00:00+00 | 0x51b50981730952631b8995d28473db12c3add290 |  0.003839107445501847 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0xfa371f331541a96957b0e9d2e51cad3ecef47caa |      1.84170753435371 | {beefy-binspirit}
 2022-06-30 00:00:00+00 | 0xd75bb9a345ed7bdeed2b17bf1bb545fd3399bb14 |     3.824807137321156 | {nfty-nfty}
 2022-06-30 00:00:00+00 | 0xd10a49f8a5ceac469ee7a6157ec5d3059805f5e3 |   0.24977955110741856 | {boo-wftm-beftm}
 2022-06-30 00:00:00+00 | 0xaa001863ef8ee2ff2e500ec7811bfe89ab2f0d8b |     4.309784281024348 | {geist-ftm}
 2022-06-30 00:00:00+00 | 0x5b11abcc2e69195f4b5da2aa486eddc260a90e1f |    1.2614758063618166 | {blizz-blzz-avax}
 2022-06-30 00:00:00+00 | 0xe9504198bfa4066ed26b79c8280bd51ec6bd9af9 | 0.0017381808971092887 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x218f4ebb76d792da3869c01a7b06a020c9820db8 |  0.006941891832475075 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x5df212f40a87d7feb1cc139e36614413e882f304 |     4.994189353167237 | {pearzap-pear-matic}
 2022-06-30 00:00:00+00 | 0x569be03c540e805811b0f46caea55e1bd1e1e1b0 |    0.0894925707052709 | {nfty-nfty}
 2022-06-30 00:00:00+00 | 0x52eacc0efbd89bbf6018cbead6113c32f105863b |   0.04724707816778045 | {spirit-dai-usdc}
 2022-06-30 00:00:00+00 | 0xe7b234b7584cb0796d4b893f2dec78b90f01643c |      4.81477251709324 | {tomb-tomb-wftm}
 2022-06-30 00:00:00+00 | 0x7185e7deb001251901d92d9586d855b192cc43b9 |     8.020256830892043 | {geist-geist-ftm}
 2022-06-30 00:00:00+00 | 0xad1a74a31b00ed0403bb7d8b11130e30ae15853c |     5.675792077914714 | {png-png}
 2022-06-30 00:00:00+00 | 0xc09cabd7b890994145f88102dc7f380f227f0e53 |     7.718241600527816 | {beefy-beqi}
 2022-06-30 00:00:00+00 | 0x9728dcba1d289cb49617cd3735fc3499db1977aa |     4.512899829693158 | {cakev2-btcb-bnb}
 2022-06-30 00:00:00+00 | 0x0fb947c98f86cba8520a37bfc187d4dc4178556e |    3.9925366504261737 | {yel-yel}
 2022-06-30 00:00:00+00 | 0x1b2f74c5a605c72fdfa1d1e88ecf8c44ee7f718e |     8.077639763574052 | {2omb-2omb-ftm-eol}
 2022-06-30 00:00:00+00 | 0x11f6c932b97524b431d3ed92fe58bbd12c1f39c9 |   0.12704532153709763 | {stellaswap-luna-wglmr-eol}
 2022-06-30 00:00:00+00 | 0xcc4f4516b6d2c206e0e5628ce4b14d29e9ed9bf8 |  0.002707427038430772 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x2713508691580c6d8916d47b08e5ae599cfc0613 |    2.8992595817108455 | {dibs-dshare-bnb}
 2022-06-30 00:00:00+00 | 0x9373bdbc98c8ee61b5aec7fbab6763b165bb2c99 |     1.440032397341642 | {boo-boo-ftm}
 2022-06-30 00:00:00+00 | 0xa521e425f37acc731651565b41ce3e5022274f4f |     5.982364713395869 | {avax-bifi-maxi}
 2022-06-30 00:00:00+00 | 0x18727512ef3ef75e6d9a67295bd9926c7d45fc9d |     8.243872201882688 | {banana-bananav2}
 2022-06-30 00:00:00+00 | 0xfbae08044617f701eaedc88ee3289ad9902bbfca |   0.33817504844874413 | {mdex-bsc-mdx}
 2022-06-30 00:00:00+00 | 0x1d1570ffb8cf479e7f3dd7ebfa5a3f222bc042a6 |    3.4612338231200566 | {banana-watch-bnb}
 2022-06-30 00:00:00+00 | 0x68ee4b79850f6b172e07115badfd07f90bfcdae7 |    3.7198034480810267 | {ripae-pftm-ftm,tomb-tomb-mai}
 2022-06-30 00:00:00+00 | 0x345c190cdfb5f8d067e0274a75bb7e6db4404b3d |   0.36135056847480357 | {blizz-blzz-avax}
 2022-06-30 00:00:00+00 | 0x279085d1956c132da8dd29ed30ffa8ff1b777629 | 0.0018137435003962447 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x8aab65fd0ec05a3e6ef0f575e5ad3c6adc8cd0fa |     5.235910228533907 | {quick-quick}
 2022-06-30 00:00:00+00 | 0x2614aeda2033314531ca34521aed77b3cfafad9f |     9.520113966851193 | {ripae-pae-ftm,geist-ftm}
 2022-06-30 00:00:00+00 | 0x1be7405d3998fc6bb885b142b27e3256276274f6 |  0.002513986406892517 | {ape-banana-matic}
 2022-06-30 00:00:00+00 | 0x74d4510e28a236d2fb7b2fa33c4fba5bd44b252d |    0.3387456320363735 | {yel-yel}
 2022-06-30 00:00:00+00 | 0x9fe59aa9f3d813e0d15916d49648a52187c58eb0 |  0.011482872143754602 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x1805de850c478769860504978770d9afbf082071 |  0.014370785113335713 | {joe-joe}
 2022-06-30 00:00:00+00 | 0x824202e7d282a207d04f121b32980af3d6287397 |     1.536042117941699 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x315ee73b2abd4036173b35348501c45bcfc235f0 |     6.511950223170424 | {dinoswap-dino-eth-eol}
 2022-06-30 00:00:00+00 | 0x5e66084781c5c28140a48f53ef5f97a6c9e1ba5a |    2.6543409308869004 | {ripae-pftm-ftm}
 2022-06-30 00:00:00+00 | 0xe741e4c5277088a138d5708e8b54571c5e0200a6 |   0.07582452409144062 | {scream-crv}
 2022-06-30 00:00:00+00 | 0xcb4a2936156d1de6c10037e13ad4c99ccf3c2da3 |      2.67808892170127 | {banana-ont-bnb}
 2022-06-30 00:00:00+00 | 0x33bdaeb74bf5430695773eeaea2cdf05117e0046 |     9.238543385671777 | {ripae-pae-ftm}
 2022-06-30 00:00:00+00 | 0x6c27a76749347c596c28364db1249538cdec45ae |    2.1294894271625977 | {baby-baby}
 2022-06-30 00:00:00+00 | 0xc4ec10e6263fc7ccb9055d6b4c8b2a8ecc0d719e |     2.820985995362011 | {blizz-blzz-avax}
 2022-06-30 00:00:00+00 | 0x0eace3577f013699322972eda811ac781bc0db84 |     6.079684556113799 | {vvs-cro-atom}
 2022-06-30 00:00:00+00 | 0xd0d211f28f0116161abe9334476dc3fa32115fe6 |     7.072256469982932 | {0xdao-tomb-eol,beets-fidelio-duetto,tomb-tshare-ftm-eol,jetswap-fantom-fwings-eol,beets-battle-bands-eol}
 2022-06-30 00:00:00+00 | 0x9eed5584122339bb79932e8cf1f2a991c2f83926 |     4.358279148041811 | {stellaswap-stella-wglmr}
 2022-06-30 00:00:00+00 | 0x8feb82b6618bb2f1281ee861ef3a4d12679fbd12 |    0.7542369395175248 | {banana-bananav2}
 2022-06-30 00:00:00+00 | 0xec89aab69394c32ce9429b1a2ebea86328c939c0 |     2.952477681792109 | {wsg-wsg,banana-bananav2}
 2022-06-30 00:00:00+00 | 0x2687bc562564cf0608960c0987b78b3423c1595b |   0.15619911348785442 | {2omb-2omb-2share-eol}
 2022-06-30 00:00:00+00 | 0xba9c4d1df8c2f7d1ebf3a9f6b34d04a552aabd6e |  0.003278845789587044 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0x4e7d85f8df2fe77c2dc7cbe0344652914d61a6cd |    0.9199309208939999 | {polygon-bifi-maxi}
 2022-06-30 00:00:00+00 | 0x43601a8a86f62bff4cd5755fd292479cca3260a5 |     0.163539950465721 | {liq-liq-cro}
 2022-06-30 00:00:00+00 | 0x550d19253fb509fdbd67e1eb8b114107058131ef |   0.02907851153537558 | {jetswap-fantom-fwings-eol}
 2022-06-30 00:00:00+00 | 0x7eda7c3e1ed86ba18208793e673eb10df9b98ec8 |     4.297880493741908 | {crona-crona,crona-cro-crona}
 2022-06-30 00:00:00+00 | 0x153a0ca6558387ea74cccdfc68452885a9f48cf2 |    1.5185984504604702 | {czf-czf-bnb}
 2022-06-30 00:00:00+00 | 0xde94b73c76c96b35a7ab5e591f5703e20490c20d |     3.227494981391513 | {charge-charge-busd}
 2022-06-30 00:00:00+00 | 0xed48f60fe843c65870ee14ef0570a3668c8fd612 | 7.556422532498493e-06 | {emp-eshare-wbnb}
 2022-06-30 00:00:00+00 | 0x98df23fc2378af038b56e031a11b3aec03d87c47 |    6.8163654655795325 | {fuse-fuse}
 2022-06-30 00:00:00+00 | 0xe825b40121f305f8ee7da41255175b2c8a27d829 |    2.8475538090714227 | {beets-battle-bands-eol}
 2022-06-30 00:00:00+00 | 0xfbf2118e87242235f3e15a022496976a495fc7a1 |     3.808873697455614 | {cakev2-cake-bnb}
 2022-06-30 00:00:00+00 | 0x81164a89578d7712f3fffb763587f91f0a311854 |     3.283357599099003 | {polygon-bifi-maxi}
 2022-06-30 00:00:00+00 | 0xd98954846d85b178c4120a18549aab1f104e1f6d |  0.014141666945835667 | {baby-baby-usdt,cakev2-chess-usdc,cakev2-idia-busd-eol,tenfi-tenfi-busd,banana-banana-busd,ooe-ooe-busd}
 2022-06-30 00:00:00+00 | 0xad6082d3fc4c789f257319d4e80dc904512a07ca |    2.7489798714993903 | {vvs-vvs-usdt}
 2022-06-30 00:00:00+00 | 0xd3cdb0612c0f805c9cc0e5e67fa30e6448601860 |    0.4307954033489955 | {sushi-arb-magic-weth}
 2022-06-30 00:00:00+00 | 0x38229ca0c7b5e26952bec0fd81ec031c9ad788f7 |     8.303879972009753 | {aavev3-avax}
 2022-06-30 00:00:00+00 | 0xd19c64f4b56e84952c2edca7457c668492417686 |     7.168261815086995 | {ripae-pftm-ftm}
 2022-06-30 00:00:00+00 | 0x68b27faa96b1feb724626112051a15a8e7c5b8d4 |    0.1165241730809328 | {banana-bananav2}
 2022-06-30 00:00:00+00 | 0xb127e77c3d390d1fedf325203233597a9e45bf65 |  0.003879169749866304 | {pacoca-pacoca}
 2022-06-30 00:00:00+00 | 0xeacd0167ca166f82d7526b3ef08aecafb33dfb67 |  0.021120076475860186 | {nfty-nfty}
 2022-06-30 00:00:00+00 | 0x253291a87f13c9c13c15538c7b3599ed7fdc010d |  0.011352562350564683 | {sushi-celo-cusd-ceur-eol}
 2022-06-30 00:00:00+00 | 0x16468da20bf18dab2cbf563ead564c34f402c64e |    3.5280787227605024 | {cronos-bifi-maxi}
 2022-06-30 00:00:00+00 | 0xbf6bdb7175fc033a7387d94f54b129800e8c7c9f |  0.015387390617089453 | {jetswap-poly-pwings-eol}
 2022-06-30 00:00:00+00 | 0x826c2691e8d3cc8d60867b31828984532ad8df73 |    1.7182335436752083 | {crona-crona}
 2022-06-30 00:00:00+00 | 0xa76bafe91e80e852386facc2ad1be55089e7635f |     5.022174747104075 | {based-based-tomb}
 2022-06-30 00:00:00+00 | 0x1b857e88dee65ef699fbe8c46dee5e51a1790338 |     5.372946342970096 | {crona-cro-crona}
 2022-06-30 00:00:00+00 | 0x5700d3b0d7c4b2d65e0bec08ba4bf0ea9f98be9f |    5.7253543570320184 | {bifi-maxi}
 2022-06-30 00:00:00+00 | 0x451c79fbabb67c0daebe4211c14146054aa58a1e | 0.0018083582373589688 | {wsg-wsg}
 2022-06-30 00:00:00+00 | 0xdf11a237ba2fe744593789436f385f0734f18d00 |    0.2083457713546752 | {beefy-binspirit}
 2022-06-30 00:00:00+00 | 0x079111de52742b7334a6d77aa6a3ed2efd009431 |    0.7737479046806354 | {blizz-blzz-avax}
(100 rows)
