```

IMPORTANT:
[ ] Find a way to limit cpu and memory usage for timescale and grafana
[ ] Deploy timescaledb and grafana to let ppl explore -> NEED iptables filters first (keep ssh on custom or dead omg)
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


beefy_db-load-timescaledb-1  | 2022-07-07T18:50:10.402Z [error] [LTSDB] Error on import stream ERC20 Transfers diffs for bsc:banana-aave-bnb:  Refusing to insert negative balance for non-mintburn address bsc:0x2EF1B47174d9f9bC6e9a37E863928C0E6100324d: {"data":{"blockNumber":19317321,"datetime":"2022-07-06T15:54:28.000Z","from":"0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e","to":"0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362","value":"50740500578502475"},"lastBalance":"0","newBalance":"-50740500578502475"}
{"id":"banana-aave-bnb","token_name":"mooApeAAVE-BNB","token_decimals":18,"token_address":"0x2EF1B47174d9f9bC6e9a37E863928C0E6100324d","want_address":"0xf13e007e181A8F57eD3a4D4CcE0A9ff9E7241CEf","want_decimals":18,"price_oracle":{"want_oracleId":"banana-aave-bnb","assets":["AAVE","BNB"]}}

cat ../indexed-data/chain/bsc/contracts/0x2EF1B47174d9f9bC6e9a37E863928C0E6100324d/ERC20/Transfer.csv | grep 0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e
15870198,2022-03-08T04:18:15.000Z,0x0000000000000000000000000000000000000000,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,48275731502199089889
15952102,2022-03-11T01:08:13.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x0000000000000000000000000000000000000000,99451912786065947
16099271,2022-03-16T04:09:19.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x0000000000000000000000000000000000000000,112572526355436766
16264886,2022-03-21T23:26:39.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x0000000000000000000000000000000000000000,97373726464065207
16439004,2022-03-28T01:10:55.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,112075978860115448
16661934,2022-04-04T21:05:34.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,149050885415878541
17072446,2022-04-19T05:02:21.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,296236807686278891
17549944,2022-05-05T21:20:19.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,272493258863938884
18430234,2022-06-05T17:53:28.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,401348382020434266
18671291,2022-06-14T04:09:35.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,87402148081155791
18863337,2022-06-20T20:55:56.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,72738091241100520
19042563,2022-06-27T02:37:53.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,43596239293996549
19121656,2022-06-29T20:38:26.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,36303556210293327
19317321,2022-07-06T15:54:28.000Z,0x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e,0x10Ab57c5889A00C497CE6BE7Ce142BB9A613E362,50740500578502475


        SELECT *
        from data_raw.erc20_balance_diff_ts
        where chain = 'bsc'
          and contract_address = '\x2EF1B47174d9f9bC6e9a37E863928C0E6100324d'
          and owner_address = '\x38D8D19dA94cc39FC719Cb559eb2cDa6204e9F3e'
          order by datetime asc
        group by owner_address




ERROR:  operand, lower bound, and upper bound cannot be NaN


ERROR on vault stats reload
beefy_db-load-timescaledb-1  | 2022-07-07T18:26:27.266Z [info] [DB] Refreshing vault stats for vault bsc:biswap-fil-usdt (146/597)
beefy_db-load-timescaledb-1  | 2022-07-07T18:26:27.279Z [error] [MAIN] ERROR
beefy_db-load-timescaledb-1  | error: operand, lower bound, and upper bound cannot be NaN
beefy_db-load-timescaledb-1  |     at Parser.parseErrorMessage (/opt/app/node_modules/pg-protocol/dist/parser.js:287:98)
beefy_db-load-timescaledb-1  |     at Parser.handlePacket (/opt/app/node_modules/pg-protocol/dist/parser.js:126:29)
beefy_db-load-timescaledb-1  |     at Parser.parse (/opt/app/node_modules/pg-protocol/dist/parser.js:39:38)
beefy_db-load-timescaledb-1  |     at Socket.<anonymous> (/opt/app/node_modules/pg-protocol/dist/index.js:11:42)
beefy_db-load-timescaledb-1  |     at Socket.emit (node:events:527:28)
beefy_db-load-timescaledb-1  |     at addChunk (node:internal/streams/readable:324:12)
beefy_db-load-timescaledb-1  |     at readableAddChunk (node:internal/streams/readable:297:9)
beefy_db-load-timescaledb-1  |     at Readable.push (node:internal/streams/readable:234:10)
beefy_db-load-timescaledb-1  |     at TCP.onStreamRead (node:internal/stream_base_commons:190:23) {
beefy_db-load-timescaledb-1  |   length: 115,
beefy_db-load-timescaledb-1  |   severity: 'ERROR',
beefy_db-load-timescaledb-1  |   code: '2201G',
beefy_db-load-timescaledb-1  |   detail: undefined,
beefy_db-load-timescaledb-1  |   hint: undefined,
beefy_db-load-timescaledb-1  |   position: undefined,
beefy_db-load-timescaledb-1  |   internalPosition: undefined,
beefy_db-load-timescaledb-1  |   internalQuery: undefined,
beefy_db-load-timescaledb-1  |   where: undefined,
beefy_db-load-timescaledb-1  |   schema: undefined,
beefy_db-load-timescaledb-1  |   table: undefined,
beefy_db-load-timescaledb-1  |   column: undefined,
beefy_db-load-timescaledb-1  |   dataType: undefined,
beefy_db-load-timescaledb-1  |   constraint: undefined,
beefy_db-load-timescaledb-1  |   file: 'float.c',
beefy_db-load-timescaledb-1  |   line: '4033',
beefy_db-load-timescaledb-1  |   routine: 'width_bucket_float8'
beefy_db-load-timescaledb-1  | }
beefy_db-load-timescaledb-1  | 2022-07-07T18:26:27.289Z [error] undefined


-------------

Error inconsistent balance on cronos

beefy_db-load-timescaledb-1  | 2022-07-07T17:17:45.481Z [error] [LTSDB] Error on import stream ERC20 Transfers diffs for cronos:vvs-vvs-usdt:  Refusing to insert negative balance for non-mintburn address
cronos:0x2425d707a5C63ff5De83eB78f63e06c3f6eEaA1c: {
    "data":{
        "blockNumber":643139,
        "datetime":"2021-12-20T06:57:24.000Z",
        "from":"0x129Dd111C23a8AE4a14694eeb5fAAd7cE9Ed19e1",
        "to":"0x0000000000000000000000000000000000000000",
        "value":"40125445583275"
    },
    "lastBalance":"0",
    "newBalance":"-40125445583275"
}



Error inconsistent balance on harmony

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



```
