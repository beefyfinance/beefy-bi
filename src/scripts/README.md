```
/data/beefy/<chain>/vaults.json
/data/<chain>/blocks/block.csv
    - block_number: int
    - block_date: datetime
/data/<chain>/contracts/<contract_address>/ERC20/Transfer.csv
    - block_number: int
    - from: addr
    - to: addr
    - value: uint256
/data/<chain>/contracts/<contract_address>/creation_date.json
    - created_at: <timestamp>
    - creation_block: <block_number>
/data/<chain>/contracts/<contract_address>/strategies.csv
    - block_number: int
    - strategy_address: addr
```
