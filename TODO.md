```

IMPORTANT:
[ ] How do we get IL and individual token balance per vault?
    [x] find out how thetopdefi got IL and token stats -> custom adapters
    [ ] wait for chebin to rework the fetchXXXPrice code so we can have LP balance of each token
[x] periodically refresh vault list
[x] fetch token and want prices from beefy api
[x] fix completeness checks for vault transaction date
[x] poc a dashboarding solution
[ ] configure rpc api keys and speed up rate limit to those, maybe even disable shared locking
[ ] Backfill price history, maybe from db archive
[ ] Fix missing ppfs on bsc for some vaults
[ ] Find a better solution than bigquery, or at least with stable costs



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

```
