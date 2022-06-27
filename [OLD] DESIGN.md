# Introduction

This documents evaluate technical solutions to fulfill the following requirements.
We prioritize fulfilling the requirements while optimising for operational efficiency.

# Requirements

## Feature 1: Platform usage and treasury inflow

- Display historical and live data per vault
  - Can be aggregated to 15 min if needed, allow for higher time frame aggregations (1d, 1w, etc)
  - Live data: allow for 1h delay
  - All data can be requested in "want" token, in native equivalent, and in USD equivalent.
  - Data needed:
    - TVL
    - Vault deposits and withdraws
    - Vault harvest (with details for treasury, harvester, strategist)
- Display revenu <> $BIFI relationship
  - bifi staker revenue <= IDK HOW THIS IS COMPUTED
  - historical buyback amount?

Questions to answer:

- the effectiveness of marketing activities on vault tvl
- the effect of launchpools, also does the tvl stay after the launchpool ends
- harvests -> purely profit, how much are we earning from stable vaults, how much are degen vaults generating, precise amount of - monthly treasury inflow? -> this then enables us to run new scenarios, if fees were modified to X, Y and Z, how would the impact on bifi stakers and treasury revenue be?

## Feature 2: Detailed tracking of individual user investments

- Display user investments history (per address)
  - Can be aggregated to 15 min if needed, allow for higher time frame aggregations (1d, 1w, etc)
  - Live data: allow for 1h delay
  - All data can be requested in "want" token, in native equivalent, and in USD equivalent.
  - Data needed:
    - Historical investment value (moo token amount and price over time)
  - Derived data
    - Average deposit price
    - P&L + IL

# Architecture

## Overview

```mermaid
flowchart RL
    subgraph blockchain
        direction TB
        Vault1 -- ... --> VaultN
    end
    subgraph project
        Indexer  --> DataStore[(Data Store)]
        API --> DataStore
    end
    subgraph clients
        WebUI
    end

    Indexer ---> blockchain
    clients ---> API
```

This project will be split in 3 parts:

- Indexer:
  - Knows how to fetch data from the blockchain
  - Is able to retrieve historical and live data
- Historical data store:
  - Be the "source of truth" for historical data
  - Needed because blockchain data access is too slow
- API:
  - provide a stable and performant interface
  - maybe do some caching

## Operational requirements/notes

- Adding a new vault should not require code change. Ideally we will maintain an on-chain registry of all vaults.
- We want to minimize interaction with the vault strategies (events and calls) as those are less standardized (api and events) than vaults.
- We want to use events as much as possible compared to contract calls as events are easier to access and more efficient.
- We can rely heavily on ERC20 events and methods to get data from the vault. This is very standard, could scale to other needs.
- We could rely on external price feeds if on-chain oracles are not available, but it's not advised.
- We do not aim to decentralize the runtime of this project, the source code will be open so anyone could run an instance.

## Individual data sources

- Moo token history amount per address
  - Result of proof of concept: can be fetched efficiently from the vault ERC20 transfer event
  - We use ERC20 transfers because most vault do not trigger an event when a deposit is made, it seems to be the strategy role to do so, so we avoid using those events (see above op req)
  - Doing so is compatible with EIP-4626
- Moo token price in "want"
  - Result of proof of concept: can be fetched at regular interval in the past by passing a block number to the contract call "pricePerFullShare()"
  - Might require a special RPC node for older vaults (archive node)
- LP token price
  - Beefy already have an LP price feed based on thegraph
  - Beefy LP price feed is maintained by strategists and is difficult to maintain, if possible, automate this process (unlikely). If not, make sure this price feed is easy to update and changes are propagated to the beefy api and data project.
- TVL (in "want") for each vault
  - call balance() on the vault contract with a block number at regular intervals in the past
- Harvests
  - Assumption: collecting harvest data only benefit Feature 1, as Feature 2 (users details) requirements are fulfilled enough by fetching pricePerFullShare()
  - Harvest amount:
    - TODO
  - Harvest fees:
    - Assumption: harvests convert all to native before splitting between harvester, strategist and treasury
    - Assumption: no custom event is reliably used enough to be used as a source of truth for fees
    - Result of proof of concept: we can use ERC20 transfer events to get the harvest details

In the future, we could derive APR and APY from this data, but it's not the goal of the first version.

## Data store capacity analysis

Data Sizing:

```
- Vaults to track: ~2000, design for ~5000
- Accounts per vault: ~500, design for ~1000. Note: popular vaults can have 10k~30k accounts
- Time sampling: 4h (>1y), 1h (>1m), 15min (<1m) timestep -> 30*24*4 + 11*30*24 + 365*6 -> 13k data points
- Data to store:
    - Deposit/withdraw:
        - ERC20 transfers from/to vaults:
            - With from/to (feat2)
                - 10~30 / account / vault -> 1000*5000*30 -> 150M data points
                - values: from/to (address 160 bit) + value (256 bit) + timestamp (64 bit)
                - dimensions: vault address (160 bit) + chain (8 bit)
                - 150M*480bit -> 9 gigabytes + dimensions
            - Without from/to (feat1)
                - 5000 vaults \* ~2000 deposit/withdraws -> ~10M data points
                - values: value (256 bit) + timestamp (64 bit)
                - dimensions: vault address (160 bit) + chain (8 bit)
                - 10M*320bit -> 400 megabytes + dimensions
    - PPFS: 1 / vault / ts point
        - values: price per full share (256 bit) + timestamp (64 bit)
        - dimensions: vault address (160 bit) + chain (8 bit)
        - 5000 * 13k -> 65M data points -> 65M*320bit -> 2.6 gigabytes + dimensions
    - Vault strat list
        - Negligible, max 5 / vault -> 25k
    - Prices:
        - ~3/vault (lp + underlying 1 + underlying 2), but many overlap
        - values: price approx usd (32 bit) + price approx native (32 bit) + timestamp (64 bit)
        - dimensions: chain (8 bit)
        - 3000 (5k - overlap) * 3 * 13k -> 117M data points -> 117M*128bit -> 1.8 gigabytes + dimensions
    - Harvests:
        - 1 / vault / ts point (aggregate if needed)
        - values: harvest approx usd (32 bit) + harvest approx native (32 bit) + split strategist/harvester/treasury 3 * 32 bit + timestamp (64 bit)
        - dimensions: chain (8 bit) + vault address (160 bit)
        - 5000 * 13k -> 65M data points -> 65M*224bit -> 1.8 gigabytes + dimensions
- Total (uncompressed):
    - Deposit/withdraws counts (feat1): ~500Mb
    - PPFS: ~3Gb
    - Prices: ~2Gb
    - Harvests: ~2Gb
```

## cloud vs on prem

```
- on prem:
    (+): cheaper to run ($100~$200 / month)
    (+): more decentralized (no dependency on specific cloud provider service -> anyone can spin an instance)
    (-): access control is more error prone
    (-): more exensive to maintain (dev time), need upgrades
    (-): need to test and maintain timeserie databases
    (~): forces us to produce more efficient code
- cloud:
    (-): more expensive to run ($300~$1000 / month) and c
    (-): less decentralized (dependency on specific cloud provider service)
    (+): access control is less error prone (revokable access)
    (+): less expensive to maintain (dev time), automatic upgrades upgrades
    (+): included timeserie / big data tools
    (~): let us produce sloppy code that have an impact on infrastructure cost
```

###############################################################################################
###############################################################################################
########################### WIP ###########################
###############################################################################################
###############################################################################################

## Logical data flow

```mermaid

flowchart TB
    subgraph blockchain
        direction TB
        subgraph beefy
            Vault1 -- ... --> VaultN
        end
        subgraph chainlink
            Oracle1 -- ... --> OracleN
        end
    end
    subgraph external
        direction LR
        CoingeckoAPI
        BeefyAPI
        external_etc(...)
    end
    subgraph prices
        direction TB
        subgraph token_prices
            FTM_price_feed --> token_prices_etc(...) --> BNB_price_feed
        end
        subgraph lp_prices
            FTMUSDT_price_feed --> lp_prices_etc(...) --> BNBUSDT_prices_feed

        end
        subgraph moo_token_price
            mooCakeBNBUSDT_prices --> moo_token_price_etc(...) --> mooSpookyFTMUSDT_prices
        end
        moo_token_price ---> lp_prices
        moo_token_price ---> token_prices
        lp_prices ---> token_prices
    end
    prices ----> blockchain
    prices ----> external

    subgraph account_balance
        Vault1_balance_history --> account_balance_etc(...) --> VaultN_balance_history
        Boost1_balance_history --> account_balance_boost_etc(...) --> BoostN_balance_history
    end
    account_balance --> transfer_event("ERC20TransferEvent") --> blockchain
    account_balance --> balanceOf_call_event("balanceOf(addr).call({}, blockNumber)") --> blockchain

    subgraph portfolio_history
        User1_history --> portfolio_history_etc(...) --> UserN_history
    end
    portfolio_history ---> account_balance
    portfolio_history ---> prices


    subgraph apy_history
        Vault1_apy_history --> apy_history_etc(...) --> VaultN_apy_history
    end
    apy_history ---> prices

    subgraph clients
        WebUI
        BeefyAPI?
    end
    clients ---> portfolio_history
    clients ---> prices
    clients ---> apy_history

    subgraph missing_from_this_chart
        Harvest
    end
```

```mermaid
flowchart RL
    subgraph blockchain
        direction TB
        Vault1 -- ... --> VaultN
    end
    subgraph project
        subgraph indexer
            direction TB
            Indexer -- Generates --> IndexedData[(Indexed Data)]
        end
        subgraph merger
            direction TB
            Merger -- Derives --> DerivedData[(Derived Data)]
            Merger --> IndexedData
        end
        API -- Queries--> DerivedData
    end
    subgraph clients
        WebUI
    end

    indexer ---> blockchain
    clients ---> API
```

# Component design

TODO: design API first

- Indexer responsibility is to know which data to get and how to get it. IE: knows about vault list, knows how to get shares, etc.
- Data store exposes public raw data for each individual data sources. IE: shares history, shares to "want" rate history, "want" oracle usd price history, etc.
- Api merges these raw data and provide easy access to merged data. IE: multiply shares with "want" rate with "want" oracle price to return usd price or shares directly, etc.

## Indexer

TODO

- evaluate the graph maybe
- can we reuse the vault registry or do we need a new one? (maybe we need oracle addresses, some additional config etc)

```mermaid
flowchart RL
    subgraph blockchain
        direction TB
        VaultRegistry
        Vault1 --Triggers--> EventTransferERC20
        VaultRegistry --Knows about--> Vault1
        VaultRegistry --Knows about--> Vault2
        VaultRegistry --Knows about--> Oracles
        subgraph "Vault 1"
            direction LR
            Vault1 --> Strategy1_1
            Vault1 --> Strategy1_2
        end
        subgraph "Vault 2"
            direction RL
            Vault2 --> Strategy2_1
            Vault2 --> Strategy2_2
        end
        subgraph "Events"
            EventTransferERC20[ERC20 Transfer]
        end
        subgraph "Oracles"
            direction LR
            NativePrice
            LPPrice1
            LPPrice2
            LPPrice3
        end
    end
    subgraph project
        Indexer  --> DataStore[(Data Store)]
        API --> DataStore
    end
    subgraph clients
        WebUI
    end

    Indexer -.2. Listen to and fetches.-> EventTransferERC20
    Indexer -."3. pricePerFullShare.call(blockNumber)".-> Vault1
    Indexer -."3. balance.call(blockNumber)".-> Vault1
    Indexer -."3. price.call(blockNumber)".-> Oracles

    Indexer --1. Get config from---> VaultRegistry
    clients ---> API

```

## Data store

TODO

- Most data we need to store is actually derived data from the blockchain
- I intuitively think that storing historical data in a file format is inherently the way to go
  - Dirt cheap to operate (store, copy, backup, etc)
  - No server = no problems
  - Avoid the database war
  - We can still derive additional data and put it in an SQL db if needed
  - This is what large players in the industry use to store and distribute historical data (see: https://public.bybit.com)
  - Other projects could easily integrate with a public file server directly, less likely with a database as they will need to rely on the api
  - Having an SQL server is what people expect though
- Data store will ultimately depend on how the data is used

## API

TODO

- should we return JSON (more standard) or CSV (streamable, concatenable, a bit more difficult to parse)

### Requirements / Notes

Operational requirements:

- Need to scale
- Most calls should return from cache
- Resilient to updates and crashes (fast startup)

Notes:

- Values are expressed as string decimals with a dot as separator. Ex: "1023212323.23456789". Spec below uses "Decimal"
- Vault id is the checksumed vault address. Ex: "0x1234567890123456789012345678901234567890". Spec below uses "Address"

Design decisions:

- As a result of operational requirements, we choose to expose low level data structures directly and let clients built on top of them. This decision will allow us to maintain a very generic API that will require less code changes to support new features. The downside of this decision it that it will require clients more code to support a single end user feature. Ex: we expose shares history, shares "want" rate and "want" usd rate, it is up to the client to multiply those values to display the total amount

### REST interface

#### Feature 1

#### Feature 2

```
GET /api/shares-history/:owner_address/:chain/:vault_address?agg=:agg&currency=:currency&before=:before&after=:after
    Where:
        - chain: "fantom", "bsc", ...
        - owner_address: address of the user, owner of moo token
        - vault_address: address of the target erc20 moo token
        - agg: "15m", "1h", "4h", "1d", "1w", "1m", "1y"
        - currency: "usd", "want", "native"
        - before: ISO8601 UTC date and time (inclusive)
        - after: ISO8601 UTC date and time (inclusive)
    Returns:
        - historical data in the requested currency ordered by date
        - null if not yet invested, 0 if fully withdrawn
        - we choose a columnar representation to have less bytes to transfer compared to a list of js objects
        {
            dates: Array<Date>,
            shares: Array<Decimal|null>,            // amount of shares owned by the user
            usd_value: Array<Decimal|null>,         // us dollars equivalent of the shares
            underlying_value: Array<Decimal|null>,  // underlying token amount equivalent of the shares
            native_value: Array<Decimal|null>,      // native amount equivalent of the shares
        }

GET /api/shares-history/:owner_address/:chain?agg=:agg&currency=:currency&before=:before&after=:after
    Where:
        - chain: "fantom", "bsc", ...
        - owner_address: address of the user, owner of moo token
        - agg: "15m", "1h", "4h", "1d", "1w", "1m", "1y"
        - currency: "usd", "native"
        - before: ISO8601 UTC date and time (inclusive)
        - after: ISO8601 UTC date and time (inclusive)
    Returns:
        - aggregated historical data over all vaults of a chain for this user
        - null if not yet invested, 0 if fully withdrawn
        - we choose a columnar representation to have less bytes to transfer compared to a list of js objects
        {
            invested_in: Array<Address>             // list of vaults where the user has positive investment in
            dates: Array<Date>,
            shares: Array<Decimal|null>,            // amount of shares owned by the user
            usd_value: Array<Decimal|null>,         // us dollars equivalent of the shares
            underlying_value: Array<Decimal|null>,  // underlying token amount equivalent of the shares
            native_value: Array<Decimal|null>,      // native amount equivalent of the shares
        }

```
