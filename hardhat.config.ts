import * as dotenv from "dotenv";
import { InfluxDB, Point, HttpError } from "@influxdata/influxdb-client";
import * as pgcs from "pg-connection-string";
import { Pool, PoolConfig } from "pg";
import pgf from "pg-format";

import { HardhatUserConfig, task } from "hardhat/config";
import "@nomiclabs/hardhat-etherscan";
import "@nomiclabs/hardhat-waffle";
import "@nomiclabs/hardhat-web3";
import "@typechain/hardhat";
import "hardhat-gas-reporter";
import "solidity-coverage";
import * as fs from "fs";
import lodash from "lodash";
import { BigNumber } from "bignumber.js";

const BeefyVaultV6_abi = JSON.parse(
  fs.readFileSync(
    __dirname + "/src/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json",
    "utf8"
  )
);

dotenv.config();

// This is a sample Hardhat task. To learn how to create your own go to
// https://hardhat.org/guides/create-task.html
task("accounts", "Prints the list of accounts", async (taskArgs, hre) => {
  const accounts = await hre.ethers.getSigners();

  for (const account of accounts) {
    console.log(account.address);
  }
});

task("past-call", "Calls a method in the past", async (_, { web3 }) => {
  const vaultAddr = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  const pastTrx =
    "0xfe5ef21a5966c3c80066f7677c3d494a96715554976d8948d87addd5b870ee35";

  let tx = await web3.eth.getTransaction(pastTrx);
  console.log(tx.blockNumber);

  const vault = new web3.eth.Contract(BeefyVaultV6_abi, vaultAddr);
  const res = await vault.methods.balance().call();
  console.log(res);
  const resPast = await vault.methods.balance().call({}, tx.blockNumber);
  console.log(resPast);
});

task(
  "list-trx",
  "List transactions that affected a contract",
  async (_, { web3 }) => {
    const vaultAddr = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
    const creationTransaction =
      "0x252603beb7b216bacc3b60cfb352496d53879d700e194aa4d7ea8f2f915f6aad";
    const lastTransaction =
      "0x3a2a600ad6d6df1638a584a273d887556dca170b6cd2696b3b9164fc191bc619";
    const vaultAddrLower = vaultAddr.toLocaleLowerCase();
    const nullAddrLower =
      "0x0000000000000000000000000000000000000000".toLocaleLowerCase();

    const createTx = await web3.eth.getTransaction(creationTransaction);
    const lastTx = await web3.eth.getTransaction(lastTransaction);

    const firstBlockNum = createTx.blockNumber || 0;
    const lastBlockNum = lastTx.blockNumber || 0;

    const vault = new web3.eth.Contract(BeefyVaultV6_abi, vaultAddr);
    const range = lodash.range(firstBlockNum, lastBlockNum + 1, 3000);

    let all_contract_trx: string[] = [];
    let all_contract_trx_blocks: number[] = [];
    for (let i = 1; i < range.length; i++) {
      const events = await vault.getPastEvents("Transfer", {
        fromBlock: range[i - 1],
        toBlock: range[i],
      });
      console.log({
        fromBlock: range[i - 1],
        toBlock: range[i],
        eventCount: events.length,
      });
      for (const event of events) {
        all_contract_trx.push(event.transactionHash);
        all_contract_trx_blocks.push(event.blockNumber);
        console.log(
          event.returnValues.from +
            " ---- (" +
            event.returnValues.value +
            ") ---> " +
            event.returnValues.to
        );
      }
    }

    all_contract_trx = lodash.uniq(all_contract_trx);
    all_contract_trx_blocks = lodash.uniq(all_contract_trx_blocks);
    console.log(all_contract_trx_blocks);
    console.log(all_contract_trx);
    console.log(all_contract_trx.length);
  }
);

task("vault-stats", "Get Historical vault stats", async (_, { web3 }) => {
  //const vaultAddr = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
  //const firstTxHash = // this can be found using getPoolCreationTimestamp.js
  //  "0x252603beb7b216bacc3b60cfb352496d53879d700e194aa4d7ea8f2f915f6aad";
  //const lastTxHash = // could be found using explorer api
  //  "0x3a2a600ad6d6df1638a584a273d887556dca170b6cd2696b3b9164fc191bc619";

  const vaultAddr = "0x41D44B276904561Ac51855159516FD4cB2c90968";
  const firstTxHash = // this can be found using getPoolCreationTimestamp.js
    "0x54e27499be2f8284f59343e0b411f7cafacede6047190e8be4f7721b2ea85931";
  const lastTxHash = // could be found using explorer api
    "0x80460fb380355d6711d4d24240e73168ca2192696806af2264f4b50029f92c41";

  const BIG_ZERO = new BigNumber(0);
  const mintBurnAddr = "0x0000000000000000000000000000000000000000";
  const mintBurnAddrLower = mintBurnAddr.toLocaleLowerCase();

  const firstTx = await web3.eth.getTransaction(firstTxHash);
  const firstBlockNum = firstTx.blockNumber || 0;
  const lastTx = await web3.eth.getTransaction(lastTxHash);
  const lastBlockNum = lastTx.blockNumber || 0;

  // we will need to call the contract to get the ppfs at some point
  const vault = new web3.eth.Contract(BeefyVaultV6_abi, vaultAddr);

  // iterate through block ranges
  const rangeSize = 3000; // big to speed up, not to big to avoid rpc limitations
  const flat_range = lodash.range(firstBlockNum, lastBlockNum + 1, rangeSize);
  const ranges: { fromBlock: number; toBlock: number }[] = [];
  for (let i = 0; i < flat_range.length - 1; i++) {
    ranges.push({
      fromBlock: flat_range[i],
      toBlock: flat_range[i + 1] - 1,
    });
  }
  console.log({ ranges: ranges.length });

  type UserBalances = { [userAddr: string]: BigNumber };
  const currentUserBalance: UserBalances = {};
  const history: {
    [blockNumber: number]: { ppfs: BigNumber; balances: UserBalances };
  } = {};

  for (const blockRange of ranges) {
    const events = await vault.getPastEvents("Transfer", blockRange);
    console.log(blockRange, events.length);

    // we could also use the RPC to call balanceOf({}, blockRange.toBlock)
    // but this is much faster. Using the RPC is more flexible though.
    // as it allows us to ignore contract logic
    for (const event of events) {
      const from = event.returnValues.from.toLocaleLowerCase();
      const to = event.returnValues.to.toLocaleLowerCase();
      const value = new BigNumber(event.returnValues.value);

      if (currentUserBalance[to] === undefined) {
        currentUserBalance[to] = BIG_ZERO;
      }
      if (currentUserBalance[from] === undefined) {
        currentUserBalance[from] = BIG_ZERO;
      }

      // mint
      if (from === mintBurnAddrLower) {
        currentUserBalance[to] = currentUserBalance[to].plus(value);
        // burn
      } else if (to === mintBurnAddrLower) {
        currentUserBalance[from] = currentUserBalance[from].minus(value);
        // transfer
      } else {
        currentUserBalance[from] = currentUserBalance[from].minus(value);
        currentUserBalance[to] = currentUserBalance[to].plus(value);
      }
    }

    const ppfs = await vault.methods
      .getPricePerFullShare()
      .call({}, blockRange.toBlock);

    history[blockRange.fromBlock] = {
      ppfs: new BigNumber(ppfs),
      balances: lodash.cloneDeep(currentUserBalance),
    };
  }
  console.log(history);

  // now format the data per block per user
  const allUsers = lodash.sortedUniq(lodash.keys(currentUserBalance));
  const allBlocks: number[] = lodash.sortedUniq(
    lodash.keys(history).map(Number)
  );
  console.log({ blocks: allBlocks.length, users: allUsers.length });

  let csv = "address;" + allBlocks.join(";") + "\n";
  // create a PPFS row
  csv +=
    "PPFS;" +
    allBlocks.map((block) => history[block].ppfs.toString(10)).join(";") +
    "\n";

  for (const user of allUsers) {
    const row =
      user +
      ";" +
      allBlocks
        .map((block) => history[block].balances[user]?.toString(10))
        .join(";");
    csv += row + "\n";
  }
  //console.log(csv);

  fs.writeFileSync(`${__dirname}/out/output${vaultAddr}.csv`, csv);
});

task(
  "vault-stats-influx",
  "Get Historical vault stats",
  async (_, { web3 }) => {
    const org = "beefy";
    const bucket = "beefy";
    const url = "http://localhost:8086";
    const token =
      "7P9r68AB3CSTmT_-OacAZPGwq-Drn7DiU1UmzRReKljvNVizf9gUj_9c7vnyKrMduqEr1uAq4-orR4lDzAVHLA==";
    const writeApi = new InfluxDB({ url, token }).getWriteApi(
      org,
      bucket,
      "ms"
    );

    //const vaultAddr = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
    //const firstTxHash = // this can be found using getPoolCreationTimestamp.js
    //  "0x252603beb7b216bacc3b60cfb352496d53879d700e194aa4d7ea8f2f915f6aad";
    //const lastTxHash = // could be found using explorer api
    //  "0x3a2a600ad6d6df1638a584a273d887556dca170b6cd2696b3b9164fc191bc619";

    const vaultAddr = "0x41D44B276904561Ac51855159516FD4cB2c90968";
    const firstTxHash = // this can be found using getPoolCreationTimestamp.js
      "0x54e27499be2f8284f59343e0b411f7cafacede6047190e8be4f7721b2ea85931";
    const lastTxHash = // could be found using explorer api
      "0x80460fb380355d6711d4d24240e73168ca2192696806af2264f4b50029f92c41";

    const BIG_ZERO = new BigNumber(0);
    const mintBurnAddr = "0x0000000000000000000000000000000000000000";
    const mintBurnAddrLower = mintBurnAddr.toLocaleLowerCase();

    const firstTx = await web3.eth.getTransaction(firstTxHash);
    const firstBlockNum = firstTx.blockNumber || 0;
    const lastTx = await web3.eth.getTransaction(lastTxHash);
    const lastBlockNum = lastTx.blockNumber || 0;

    // we will need to call the contract to get the ppfs at some point
    const vault = new web3.eth.Contract(BeefyVaultV6_abi, vaultAddr);

    // iterate through block ranges
    const rangeSize = 3000; // big to speed up, not to big to avoid rpc limitations
    const flat_range = lodash.range(firstBlockNum, lastBlockNum + 1, rangeSize);
    const ranges: { fromBlock: number; toBlock: number }[] = [];
    for (let i = 0; i < flat_range.length - 1; i++) {
      ranges.push({
        fromBlock: flat_range[i],
        toBlock: flat_range[i + 1] - 1,
      });
    }
    console.log({ ranges: ranges.length });

    type UserBalances = { [userAddr: string]: BigNumber };
    const currentUserBalance: UserBalances = {};
    const history: {
      [blockNumber: number]: { ppfs: BigNumber; balances: UserBalances };
    } = {};

    for (const blockRange of ranges) {
      const events = await vault.getPastEvents("Transfer", blockRange);
      console.log(blockRange, events.length);

      // we could also use the RPC to call balanceOf({}, blockRange.toBlock)
      // but this is much faster. Using the RPC is more flexible though.
      // as it allows us to ignore contract logic
      for (const event of events) {
        const from = event.returnValues.from.toLocaleLowerCase();
        const to = event.returnValues.to.toLocaleLowerCase();
        const value = new BigNumber(event.returnValues.value);

        if (currentUserBalance[to] === undefined) {
          currentUserBalance[to] = BIG_ZERO;
        }
        if (currentUserBalance[from] === undefined) {
          currentUserBalance[from] = BIG_ZERO;
        }

        // mint
        if (from === mintBurnAddrLower) {
          currentUserBalance[to] = currentUserBalance[to].plus(value);
          // burn
        } else if (to === mintBurnAddrLower) {
          currentUserBalance[from] = currentUserBalance[from].minus(value);
          // transfer
        } else {
          currentUserBalance[from] = currentUserBalance[from].minus(value);
          currentUserBalance[to] = currentUserBalance[to].plus(value);
        }
      }

      const block = await web3.eth.getBlock(blockRange.toBlock);
      const blockDate = new Date((block.timestamp as any) * 1000);

      const ppfs = new BigNumber(
        await vault.methods.getPricePerFullShare().call({}, blockRange.toBlock)
      );
      // write ppfs

      writeApi.writePoint(
        new Point("price_per_full_share")
          .tag("chain", "fantom")
          .tag("vault", vaultAddr)
          .uintField("value_uint", ppfs.toNumber())
          .timestamp(blockDate.getTime())
      );

      // write all user balances
      for (const userAddr of Object.keys(currentUserBalance)) {
        const userBalance = currentUserBalance[userAddr];
        writeApi.writePoint(
          new Point("erc20_balance")
            .tag("chain", "fantom")
            .tag("token_addr", vaultAddr)
            .uintField("value_uint", userBalance.toNumber())
            .timestamp(blockDate.getTime())
        );
      }
      await writeApi.flush();
    }
  }
);

task(
  "vault-stats-timescale",
  "Get Historical vault stats",
  async (_, { web3 }) => {
    const config = pgcs.parse("postgres://beefy:beefy@localhost:5432/beefy");
    const pool = new Pool(config as PoolConfig);

    //const vaultAddr = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
    //const firstTxHash = // this can be found using getPoolCreationTimestamp.js
    //  "0x252603beb7b216bacc3b60cfb352496d53879d700e194aa4d7ea8f2f915f6aad";
    //const lastTxHash = // could be found using explorer api
    //  "0x3a2a600ad6d6df1638a584a273d887556dca170b6cd2696b3b9164fc191bc619";

    const vaultAddr = "0x41D44B276904561Ac51855159516FD4cB2c90968";
    const firstTxHash = // this can be found using getPoolCreationTimestamp.js
      "0x54e27499be2f8284f59343e0b411f7cafacede6047190e8be4f7721b2ea85931";
    const lastTxHash = // could be found using explorer api
      "0x80460fb380355d6711d4d24240e73168ca2192696806af2264f4b50029f92c41";

    // can be found easily
    const vaultDecimals = 18;
    const wantDecimals = 18;
    const vaultTicker = "mooBooFTM-USDC";

    const BIG_ZERO = new BigNumber(0);
    const mintBurnAddr = "0x0000000000000000000000000000000000000000";
    const mintBurnAddrLower = mintBurnAddr.toLocaleLowerCase();

    const firstTx = await web3.eth.getTransaction(firstTxHash);
    const firstBlockNum = firstTx.blockNumber || 0;
    const lastTx = await web3.eth.getTransaction(lastTxHash);
    const lastBlockNum = lastTx.blockNumber || 0;

    // we will need to call the contract to get the ppfs at some point
    const vault = new web3.eth.Contract(BeefyVaultV6_abi, vaultAddr);

    // iterate through block ranges
    const rangeSize = 3000; // big to speed up, not to big to avoid rpc limitations
    const flat_range = lodash.range(firstBlockNum, lastBlockNum + 1, rangeSize);
    const ranges: { fromBlock: number; toBlock: number }[] = [];
    for (let i = 0; i < flat_range.length - 1; i++) {
      ranges.push({
        fromBlock: flat_range[i],
        toBlock: flat_range[i + 1] - 1,
      });
    }
    console.log({ ranges: ranges.length });

    type UserBalances = { [userAddr: string]: BigNumber };
    const currentUserBalance: UserBalances = {};

    // get chain_id
    const chain_res = await pool.query(
      pgf(
        `insert into "chain" (name) values (%L) 
        on conflict (name) do update set name = excluded.name 
        returning id;`,
        "fantom"
      )
    );
    const chain_id = chain_res.rows[0].id;

    // get token_id for vault
    const vault_token_res = await pool.query(
      pgf(
        `insert into "token" (ticker, chain_id, address) values (%L, %L, lower(%L)) 
        on conflict (chain_id, address) do update set address = excluded.address 
        returning id;`,
        vaultTicker,
        chain_id,
        vaultAddr
      )
    );
    const vault_token_id = vault_token_res.rows[0].id;

    const account_id_map: { [accountAddr: string]: number } = {};
    async function upsertAccount(accountAddr: string) {
      accountAddr = accountAddr.toLocaleLowerCase();
      if (account_id_map[accountAddr] === undefined) {
        const account_res = await pool.query(
          pgf(
            `insert into "account" (address) values (lower(%L)) 
            on conflict (address) do update set address = excluded.address 
            returning id;`,
            accountAddr
          )
        );
        account_id_map[accountAddr] = account_res.rows[0].id;
      }
      return account_id_map[accountAddr];
    }

    for (const blockRange of ranges) {
      const events = await vault.getPastEvents("Transfer", blockRange);
      console.log(blockRange, events.length);

      // we could also use the RPC to call balanceOf({}, blockRange.toBlock)
      // but this is much faster. Using the RPC is more flexible though.
      // as it allows us to ignore contract logic
      for (const event of events) {
        const from = event.returnValues.from.toLocaleLowerCase();
        const to = event.returnValues.to.toLocaleLowerCase();
        const value = new BigNumber(event.returnValues.value).shiftedBy(
          -vaultDecimals
        );

        if (currentUserBalance[to] === undefined) {
          currentUserBalance[to] = BIG_ZERO;
        }
        if (currentUserBalance[from] === undefined) {
          currentUserBalance[from] = BIG_ZERO;
        }

        // mint
        if (from === mintBurnAddrLower) {
          currentUserBalance[to] = currentUserBalance[to].plus(value);
          // burn
        } else if (to === mintBurnAddrLower) {
          currentUserBalance[from] = currentUserBalance[from].minus(value);
          // transfer
        } else {
          currentUserBalance[from] = currentUserBalance[from].minus(value);
          currentUserBalance[to] = currentUserBalance[to].plus(value);
        }
      }

      const block = await web3.eth.getBlock(blockRange.toBlock);
      const blockDate = new Date((block.timestamp as any) * 1000);

      const ppfs = new BigNumber(
        await vault.methods.getPricePerFullShare().call({}, blockRange.toBlock)
      );

      // write all user balances
      const values: any = [];
      for (const userAddr of Object.keys(currentUserBalance)) {
        const user_id = await upsertAccount(userAddr);

        const userBalance = currentUserBalance[userAddr];
        if (userBalance.isZero()) {
          continue;
        }
        const wantBalance = mooAmountToOracleAmount(
          vaultDecimals,
          wantDecimals,
          ppfs,
          userBalance
        );
        // TODO: batch write
        await pool.query(
          pgf(
            `insert into "account_balance" (
              time,
              chain_id,
              token_id,
              account_id,
              block_number,
              balance
            ) values (
              %L, %L, %L, %L, %L, %L
            )
            on conflict do nothing`,
            blockDate.toISOString(),
            chain_id,
            vault_token_id,
            user_id,
            blockRange.toBlock,
            userBalance.toString(10)
          )
        );
        /*
        writeApi.writePoint(
          new Point("erc20_balance")
            .tag("chain", "fantom")
            .tag("token_addr", vaultAddr)
            .uintField("value_uint", userBalance.toNumber())
            .timestamp(blockDate.getTime())
        );*/
      }
    }
  }
);

task(
  "vault-stats-timescale-change",
  "Get Historical vault stats",
  async (_, { web3 }) => {
    const config = pgcs.parse("postgres://beefy:beefy@localhost:5432/beefy");
    const pool = new Pool(config as PoolConfig);

    //const vaultAddr = "0x95EA2284111960c748edF4795cb3530e5E423b8c";
    //const firstTxHash = // this can be found using getPoolCreationTimestamp.js
    //  "0x252603beb7b216bacc3b60cfb352496d53879d700e194aa4d7ea8f2f915f6aad";
    //const lastTxHash = // could be found using explorer api
    //  "0x3a2a600ad6d6df1638a584a273d887556dca170b6cd2696b3b9164fc191bc619";

    const vaultAddr = "0x41D44B276904561Ac51855159516FD4cB2c90968";
    const firstTxHash = // this can be found using getPoolCreationTimestamp.js
      "0x54e27499be2f8284f59343e0b411f7cafacede6047190e8be4f7721b2ea85931";
    const lastTxHash = // could be found using explorer api
      "0x80460fb380355d6711d4d24240e73168ca2192696806af2264f4b50029f92c41";

    // can be found easily
    const vaultDecimals = 18;
    const wantDecimals = 18;
    const vaultTicker = "mooBooFTM-USDC";

    const BIG_ZERO = new BigNumber(0);
    const mintBurnAddr = "0x0000000000000000000000000000000000000000";
    const mintBurnAddrLower = mintBurnAddr.toLocaleLowerCase();

    const firstTx = await web3.eth.getTransaction(firstTxHash);
    const firstBlockNum = firstTx.blockNumber || 0;
    const lastTx = await web3.eth.getTransaction(lastTxHash);
    const lastBlockNum = lastTx.blockNumber || 0;

    // we will need to call the contract to get the ppfs at some point
    const vault = new web3.eth.Contract(BeefyVaultV6_abi, vaultAddr);

    // iterate through block ranges
    const rangeSize = 3000; // big to speed up, not to big to avoid rpc limitations
    const flat_range = lodash.range(firstBlockNum, lastBlockNum + 1, rangeSize);
    const ranges: { fromBlock: number; toBlock: number }[] = [];
    for (let i = 0; i < flat_range.length - 1; i++) {
      ranges.push({
        fromBlock: flat_range[i],
        toBlock: flat_range[i + 1] - 1,
      });
    }
    console.log({ ranges: ranges.length });

    // get chain_id
    const chain_res = await pool.query(
      pgf(
        `insert into "chain" (name) values (%L) 
        on conflict (name) do update set name = excluded.name 
        returning id;`,
        "fantom"
      )
    );
    const chain_id = chain_res.rows[0].id;

    function insert_balance_delta(
      date: Date,
      accountAddr: string,
      value: BigNumber
    ) {
      return pool.query(
        pgf(
          `insert into "erc20_balance_delta" (
            time,
            chain_id,
            token_address,
            account_address,
            balance_delta 
          ) values (
            %L, %L, lower(%L), lower(%L), %L
          )
          on conflict do nothing`,
          date.toISOString(),
          chain_id,
          vaultAddr,
          accountAddr,
          value.toString(10)
        )
      );
    }

    const blockDates: { [blockNumber: number]: Date } = {};
    async function getBlockDate(blockNumber: number) {
      if (!blockDates[blockNumber]) {
        const block = await web3.eth.getBlock(blockNumber);
        const blockDate = new Date((block.timestamp as any) * 1000);
        blockDates[blockNumber] = blockDate;
      }
      return blockDates[blockNumber];
    }

    for (const blockRange of ranges) {
      const events = await vault.getPastEvents("Transfer", blockRange);
      console.log(blockRange, events.length);

      // we could also use the RPC to call balanceOf({}, blockRange.toBlock)
      // but this is much faster. Using the RPC is more flexible though.
      // as it allows us to ignore contract logic
      for (const event of events) {
        const from = event.returnValues.from.toLocaleLowerCase();
        const to = event.returnValues.to.toLocaleLowerCase();
        const value = new BigNumber(event.returnValues.value).shiftedBy(
          -vaultDecimals
        );
        const blockDate = await getBlockDate(event.blockNumber);

        if (from !== mintBurnAddrLower) {
          await insert_balance_delta(blockDate, from, value.negated());
        }

        if (to !== mintBurnAddrLower) {
          await insert_balance_delta(blockDate, from, value);
        }
      }
    }
  }
);
// You need to export an object to set up your config
// Go to https://hardhat.org/config/ to learn more

const accounts =
  process.env.PRIVATE_KEY !== undefined ? [process.env.PRIVATE_KEY] : [];

const config: HardhatUserConfig = {
  networks: {
    bsc: {
      url: "https://bsc-dataseed2.defibit.io/",
      chainId: 56,
      accounts,
    },
    heco: {
      url: "https://http-mainnet-node.huobichain.com",
      chainId: 128,
      accounts,
    },
    avax: {
      url: "https://rpc.ankr.com/avalanche",
      chainId: 43114,
      accounts,
    },
    polygon: {
      url: "https://polygon-rpc.com/",
      chainId: 137,
      accounts,
    },
    fantom: {
      // url: "https://rpc.ftm.tools",
      url: "https://rpc.ankr.com/fantom",
      chainId: 250,
      accounts,
    },
    harmony: {
      url: "https://api.s0.t.hmny.io/",
      chainId: 1666600000,
      accounts,
    },
    arbitrum: {
      url: "https://arb1.arbitrum.io/rpc",
      chainId: 42161,
      accounts,
    },
    moonriver: {
      url: "https://rpc.moonriver.moonbeam.network",
      chainId: 1285,
      accounts,
    },
    celo: {
      url: "https://forno.celo.org",
      chainId: 42220,
      accounts,
    },
    cronos: {
      // url: "https://evm-cronos.crypto.org",
      // url: "https://rpc.vvs.finance/",
      url: "https://cronosrpc-1.xstaking.sg/",
      chainId: 25,
      accounts,
    },
    localhost: {
      url: "http://127.0.0.1:8545",
      timeout: 300000,
      accounts: "remote",
    },
    testnet: {
      url: "https://data-seed-prebsc-1-s1.binance.org:8545/",
      chainId: 97,
      accounts,
    },
    kovan: {
      url: "https://kovan.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161",
      chainId: 42,
      accounts,
    },
    aurora: {
      url: "https://mainnet.aurora.dev/Fon6fPMs5rCdJc4mxX4kiSK1vsKdzc3D8k6UF8aruek",
      chainId: 1313161554,
      accounts,
    },
    fuse: {
      url: "https://rpc.fuse.io",
      chainId: 122,
      accounts,
    },
    metis: {
      url: "https://andromeda.metis.io/?owner=1088",
      chainId: 1088,
      accounts,
    },
    moonbeam: {
      url: "https://rpc.api.moonbeam.network",
      chainId: 1284,
      accounts,
    },
  },
  gasReporter: {
    enabled: process.env.REPORT_GAS !== undefined,
    currency: "USD",
  },
  etherscan: {
    apiKey: process.env.ETHERSCAN_API_KEY,
  },
  solidity: {
    compilers: [
      {
        version: "0.8.11",
        settings: {
          optimizer: {
            enabled: true,
            runs: 200,
          },
        },
      },
    ],
  },
};

export default config;

function mooAmountToOracleAmount(
  mooTokenDecimals: number,
  depositTokenDecimals: number,
  ppfs: BigNumber,
  mooTokenAmount: BigNumber
) {
  // go to chain representation
  const mooChainAmount = mooTokenAmount.shiftedBy(mooTokenDecimals);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.multipliedBy(ppfs);

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount
    .shiftedBy(-depositTokenDecimals)
    .decimalPlaces(depositTokenDecimals);

  return oracleAmount;
}
