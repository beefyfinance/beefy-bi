IMPORTANT:
[x] periodically refresh vault list
[ ] fetch token and want prices from beefy api
[x] fix completeness checks for vault transaction date
[x] poc a dashboarding solution
[ ] Batch RPC requests? might be good for ppfs if rpcs allow it
[ ] re-enable eol vaults (/!\ might need custom logic)
[ ] find an archive node for harmony and heco
[ ] enable 15min and/or 1h data points for ppfs

NICE TO HAVE:
[ ] use jsonl format for vaults to be bigquery compatible
[ ] close read/write streams when done
[ ] make sure write streams are multi-process safe
[ ] merge transfers and ppfs imports in a single container instead of one by chain
[ ] for multi-chain scripts, retry redis lock quorum errors immediately
[ ] derive moo price from ppfs and want price
[ ] have a task scheduler with a dependency graph (apache smth?) that understand when to retry immediately or not (archive node needed, quorum errors, etc)

BONUS:
[x] remove hardhat
[x] configure which chain has a proper explorer instead of hardcoding it
[ ] configure which chain requires manual rpc calls instead of hardcoding it
[ ] create a store class that regroup similar csv stores code
[ ] create a common ArchiveNode needed error handler and logger
