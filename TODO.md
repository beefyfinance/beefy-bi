````

IMPORTANT:
[ ] How do we get IL and individual token balance per vault?
    [x] find out how thetopdefi got IL and token stats -> custom adapters
    [ ] wait for chebin to rework the fetchXXXPrice code so we can have LP balance of each token
[x] periodically refresh vault list
[ ] fetch token and want prices from beefy api
[x] fix completeness checks for vault transaction date
[x] poc a dashboarding solution
[ ] find an archive node for harmony and heco
[ ] enable 15min and/or 1h data points for ppfs
[ ] configure rpc api keys and speed up rate limit to those, maybe even disable shared locking
[ ] subscribe to web3 events to update last transaction of each vault
[ ] compute APY by vault for multiple rolling window

NICE TO HAVE:
[ ] for ppfs and native transfers imports, order vaults by last update date asc so we process first those vaults we don't have much data for. It's useful on restart so we don't have to wait so much
[ ] use jsonl format for vaults to be bigquery compatible
[ ] close read/write streams when done
[ ] make sure write streams are multi-process safe
[ ] merge transfers and ppfs imports in a single container instead of one by chain
[ ] for multi-chain scripts, retry redis lock quorum errors immediately
[ ] derive moo price from ppfs and want price
[ ] have a task scheduler with a dependency graph (apache smth?) that understand when to retry immediately or not (archive node needed, quorum errors, etc)
[ ] re-enable eol vaults (/!\ might need custom logic)

BONUS:
[x] remove hardhat
[x] configure which chain has a proper explorer instead of hardcoding it
[ ] configure which chain requires manual rpc calls instead of hardcoding it
[ ] create a store class that regroup similar csv stores code
[ ] create a common ArchiveNode needed error handler and logger
[ ] Batch RPC requests? might be good for ppfs if rpcs allow it
```
````
