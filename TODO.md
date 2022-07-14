```

IMPORTANT:
[x] Find a way to limit cpu and memory usage for timescale and grafana
[x] Deploy timescaledb and grafana to let ppl explore -> NEED iptables filters first (keep ssh on custom or dead omg)
[ ] How do we get IL and individual token balance per vault?
    [x] find out how thetopdefi got IL and token stats -> custom adapters
    [x] wait for chebin to rework the fetchXXXPrice code so we can have LP balance of each token
    [ ] Reuse chebiN code to fetch LP breakdown history
[x] periodically refresh vault list
[x] fetch token and want prices from beefy api
[x] fix completeness checks for vault transaction date
[x] poc a dashboarding solution
[ ] configure rpc api keys and speed up rate limit to those, maybe even disable shared locking
[ ] Backfill price history, maybe from db archive
[ ] Fix missing ppfs on bsc for some vaults
[ ] Find a better solution than bigquery, or at least with stable costs
[ ] Fetch all vaults from git history: some vaults are deleted from the files when they are empty
[ ] Currently we test the last transaction before fetching transfers, but we really want to save which transactions we are at once we import the transfers. Because there could be more non transfer transactions, like: https://snowtrace.io/address/0xc44d493B6219A7f5C286724b74c158CEBd7fB6f7#tokentxns


NICE TO HAVE:
[ ] for ppfs and native transfers imports, order vaults by last update date asc so we process first those vaults we don't have much data for. It's useful on restart so we don't have to wait so much
[x] use jsonl format for vaults to be bigquery compatible
[ ] close read/write streams when done
[ ] find an archive node for harmony and heco
[ ] enable 15min and/or 1h data points for ppfs
[ ] subscribe to web3 events to update last transaction of each vault
[ ] make sure write streams are multi-process safe
[ ] merge transfers and ppfs imports in a single container instead of one by chain
[ ] compute APY by vault for multiple rolling window
[ ] for multi-chain scripts, retry redis lock quorum errors immediately
[ ] derive moo price from ppfs and want price
[ ] have a task scheduler with a dependency graph (apache smth?) that understand when to retry immediately or not (archive node needed, quorum errors, etc)
[ ] re-enable eol vaults (/!\ might need custom logic)

BONUS:
[x] remove hardhat
[x] configure which chain has a proper explorer instead of hardcoding it
[ ] configure which chain requires manual rpc calls instead of hardcoding it (harmony, etc)
[ ] create a store class that regroup similar csv stores code
[ ] create a common ArchiveNode needed error handler and logger
[ ] Batch RPC requests? might be good for ppfs if rpcs allow it

```

```
[ ] Fix moonbeam data feed

Hello fellow devs, sorry for the support request :frowning: does anyone have a clue what's going on?
I'm trying to get historical data for Beefy finance, I'm calling getPricePerFullShare (0x77c7b8fc) for block 1909947 (0x1d24bb) but I get this error.

> curl -s https://rpc.api.moonriver.moonbeam.network -X POST -H "Content-Type: application/json" --data '{"method":"eth_call","params":[{"to":"0xe7123285680632e9d17a254ec7adf405559c1cbc","data":"0x77c7b8fc"},"0x1d24bb"],"id":44,"jsonrpc":"2.0"}' | jq
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32603,
    "message": "VM Exception while processing transaction: revert",
    "data": "63616e6e6f742062652063616c6c656420776974682044454c454741544543414c4c206f722043414c4c434f4445"
  },
  "id": 44
}

It looks like this work properly for previous block numbers (0x1D1F96 / 1908630) and recent block numbers (0x200DE4 / 2100708). But I get this error consistently for this specific block somehow.

Someone else at beefy did experiment with this and his conclusion is this:
> It seems to be failing in balance() which is in turn failing at want().balanceOf(address(this)) - which should be a simple map lookup on the LP _balances[account] so no reason it should fail so idk

-----------
Henry | PureStake — 06/29/2022
@MrTitoune It's because this block was produced between runtime 1503 and 1504, and we disable delegate call and callcodes during that time due to a reported vulnerability. so it's a bit of a special case.
https://github.com/PureStake/moonbeam/releases/tag/runtime-1503
```

```
-------------


ERROR:  operand, lower bound, and upper bound cannot be NaN

-------------

Sometimes, ppfs is zero
select *
from data_derived.vault_ppfs_and_price_4h_ts vpt
        where vpt.chain = 'bsc'
        and vpt.contract_address = E'\\xafE4f29578FbfE7Be32B836CBEb81daB6574cC70'
        and vpt.datetime between '2022-04-21 20:00:00' and '2022-04-22 16:00:00'
beefy-> ;
 chain |    vault_id     |              contract_address              | want_decimals |        datetime        |        avg_ppfs        | avg_want_usd_value
-------+-----------------+--------------------------------------------+---------------+------------------------+------------------------+--------------------
 bsc   | cakev2-btcb-bnb | \xafe4f29578fbfe7be32b836cbeb81dab6574cc70 |            18 | 2022-04-21 20:00:00+00 | 0.00000000000000000000 |  8694.059324844031
 bsc   | cakev2-btcb-bnb | \xafe4f29578fbfe7be32b836cbeb81dab6574cc70 |            18 | 2022-04-22 00:00:00+00 | 0.00000000000000000000 |  8672.537034078096
 bsc   | cakev2-btcb-bnb | \xafe4f29578fbfe7be32b836cbeb81dab6574cc70 |            18 | 2022-04-22 04:00:00+00 | 0.00000000000000000000 |  8708.604600668645
 bsc   | cakev2-btcb-bnb | \xafe4f29578fbfe7be32b836cbeb81dab6574cc70 |            18 | 2022-04-22 08:00:00+00 | 0.00000000000000000000 |  8686.786304415435
 bsc   | cakev2-btcb-bnb | \xafe4f29578fbfe7be32b836cbeb81dab6574cc70 |            18 | 2022-04-22 12:00:00+00 | 0.00000000000000000000 |  8675.839397862288
 bsc   | cakev2-btcb-bnb | \xafe4f29578fbfe7be32b836cbeb81dab6574cc70 |            18 | 2022-04-22 16:00:00+00 | 0.00000000000000000000 |  8561.668397043079
(6 rows)

-------------



Error inconsistent balance on harmony RPC feed

beefy_db-load-timescaledb-1  | 2022-07-07T17:22:38.501Z [error] [LTSDB] Error on import stream ERC20 Transfers diffs for harmony:sushi-one-1usdt-1usdc:  Refusing to insert negative balance for non-mintburn address
harmony:0x3ECD8Ec6BD954261397E77FD5792482Adaf2a387: {
    "data":{
        "blockNumber":21328005,"datetime":"2022-01-04T11:40:46.000Z",
        "from":"0xEc98b2e4fa05DfADc6eC76839bb70CE9751A7F20",
        "to":"0x41D44B276904561Ac51855159516FD4cB2c90968",
        "value":"3356017913"
    },
    "lastBalance":"3346748277",
    "newBalance":"-9269636"
}


beefy_db-load-timescaledb-1  | 2022-07-07T19:17:13.768Z [error] [LTSDB] Error on import stream ERC20 Transfers diffs for metis:tethys-tethys-metis:  Refusing to insert negative balance for non-mintburn address
metis:0xC8ca2254bCBA3aD8511Faff54e0b9941D0424502: {
  "data":{
    "blockNumber":925051,
    "datetime":"2022-02-17T13:26:39.000Z",
    "from":"0x6de96fe55924AF3042f218845a39FDc77d6c25fe",
      "to":"0x0000000000000000000000000000000000000000",
    "value":"120617198513072977550"
    },
    "lastBalance":"83402767084830224906",
    "newBalance": "-37214431428242752644"
}


Missing event on polygon
beefy_db-load-timescaledb-1  | 2022-07-07T19:20:00.169Z [error] [LTSDB] Error on import stream ERC20 Transfers diffs for polygon:quick-matic-usdc:  Refusing to insert nega
tive balance for non-mintburn address
polygon:0xC1A2e8274D390b67A7136708203D71BF3877f158: {
  "data":{
    "blockNumber":20675614,
    "datetime":"2021-10-27T17:59:29.000Z",
    "from":"0x48A7BEA12A800957113662d27468fD1C9E8D42Aa",
    "to":"0x0000000000000000000000000000000000000000",
    "value":"282892146405010"
  },
  "lastBalance":"187376299119262",
  "newBalance":"-95515847285748"
}
```

```
Check if PPFS is fixed for banana-guard-bnb-eol (not importing after some point)

Find a way to import the missing historical price data for polygon and BSC
```
