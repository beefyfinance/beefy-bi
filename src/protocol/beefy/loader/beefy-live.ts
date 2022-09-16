import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import {
  db_query,
  mapAddressToEvmAddressId,
  mapTransactionToEvmTransactionId,
  strArrToPgStrArr,
  vaultList$,
  withPgClient,
} from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger2";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { curry, keyBy, max, sample, sortBy } from "lodash";
import { samplingPeriodMs } from "../../../types/sampling";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MS_PER_BLOCK_ESTIMATE, RPC_URLS } from "../../../utils/config";
import { mapErc20Transfers } from "../../common/connector/erc20-transfers";
import { TokenizedVaultUserTransfer } from "../../types/connector";
import { retryRpcErrors } from "../../../utils/rxjs/utils/retry-rpc";
import { mapBeefyVaultShareRate } from "../connector/ppfs";
import { mapERC20TokenBalance } from "../../common/connector/owner-balance";
import { mapBlockDatetime } from "../../common/connector/block-datetime";
import { fetchBeefyPrices } from "../connector/prices";
import { isErrorDueToMissingDataFromNode } from "../../../lib/rpc/archive-node-needed";
import { loaderByChain } from "../../common/loader/loader-by-chain";
import Decimal from "decimal.js";
import { normalizeAddress } from "../../../utils/ethers";

const logger = rootLogger.child({ module: "import-script", component: "beefy-live" });

// remember the last imported block number for each chain so we can reduce the amount of data we fetch
type ImportState = {
  [key in Chain]: { lastImportedBlockNumber: number | null };
};
const importState: ImportState = allChainIds.reduce(
  (agg, chain) => Object.assign(agg, { [chain]: { lastImportedBlockNumber: null } }),
  {} as ImportState,
);

async function main() {
  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  const pollLiveData = withPgClient(async (client: PoolClient) => {
    const vaultPipeline$ = curry(importChainVaultTransfers)(client);
    const pipeline$ = vaultList$(client).pipe(loaderByChain(vaultPipeline$));
    return consumeObservable(pipeline$);
  });
  const pollVaultData = withPgClient(upsertVauts);
  const pollPriceData = withPgClient(fetchPrices);

  return new Promise(async () => {
    // start polling live data immediately
    //await pollVaultData();
    await pollLiveData();
    //await pollPriceData();

    // then start polling at regular intervals
    //setInterval(pollLiveData, 1000 * 30 /* 30s */);
    //setInterval(pollVaultData, samplingPeriodMs["1day"]);
    //setInterval(pollPriceData, 1000 * 60 * 5 /* 5 minutes */);
  });
}

runMain(main);

function importChainVaultTransfers(
  client: PoolClient,
  chain: Chain,
  chainVaults$: ReturnType<typeof vaultList$>,
): Rx.Observable<TokenizedVaultUserTransfer[]> {
  const rpcUrl = sample(RPC_URLS[chain]) as string;
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);

  const allTransferQueries$ = chainVaults$
    // define the scope of our pipeline
    .pipe(
      //Rx.filter((vault) => vault.chain === "avax"), //debug
      //Rx.filter((vault) => !vault.has_erc20_shares_token), // debug: gov vaults

      Rx.filter((vault) => vault.end_of_life === false), // only live vaults
    )
    .pipe(
      // batch vault config by some reasonable amount that the RPC can handle
      Rx.bufferCount(200),
      // go get the latest block number for this chain
      Rx.mergeMap(async (vaults) => {
        const latestBlockNumber = 19665966; // DEBUG
        //const latestBlockNumber = await provider.getBlockNumber();
        return vaults.map((vault) => ({ ...vault, latestBlockNumber }));
      }),

      // compute the block range we want to query
      Rx.map((vaults) =>
        vaults.map((vault) => {
          // fetch the last hour of data
          const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[chain];
          const period = samplingPeriodMs["1hour"];
          const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[chain]);

          const lastImportedBlockNumber = importState[chain].lastImportedBlockNumber ?? null;
          const diffBetweenLastImported = lastImportedBlockNumber
            ? vault.latestBlockNumber - (lastImportedBlockNumber + 1)
            : Infinity;

          const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
          const fromBlock = vault.latestBlockNumber - blockCountToFetch;
          const toBlock = vault.latestBlockNumber;

          // also wait some time to avoid errors like "cannot query with height in the future; please provide a valid height: invalid height"
          // where the RPC don't know about the block number he just gave us
          const waitForBlockPropagation = 5;
          return {
            ...vault,
            fromBlock: fromBlock - waitForBlockPropagation,
            toBlock: toBlock - waitForBlockPropagation,
          };
        }),
      ),
    );

  const [govTransferQueries$, transferQueries$] = Rx.partition(
    allTransferQueries$.pipe(Rx.mergeMap((vaults) => Rx.from(vaults))),
    (vault) => !vault.has_erc20_shares_token,
  );

  const standardTransfersByVault = transferQueries$.pipe(
    // for standard vaults, we only ignore the mint-burn addresses
    Rx.map((vault) => ({
      ...vault,
      ignoreAddresses: [normalizeAddress("0x0000000000000000000000000000000000000000")],
    })),

    // batch vault config by some reasonable amount that the RPC can handle
    Rx.bufferCount(200),

    mapErc20Transfers(
      provider,
      chain,
      (vault) => {
        if (!vault.contract_evm_address.metadata.erc20) {
          throw new Error("Vault contract is not ERC20");
        }
        return {
          address: vault.contract_evm_address.address,
          decimals: vault.contract_evm_address.metadata.erc20?.decimals,
          fromBlock: vault.fromBlock,
          toBlock: vault.toBlock,
        };
      },
      "transfers",
    ),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping erc20Transfers", data: { chain } }),

    // we also want the shares rate of each involved vault
    mapBeefyVaultShareRate(
      provider,
      chain,
      (vault) => {
        if (!vault.contract_evm_address.metadata.erc20?.decimals) {
          throw new Error("Vault contract is not ERC20");
        }
        if (!vault.underlying_evm_address.metadata.erc20?.decimals) {
          throw new Error("Underlying contract is not ERC20");
        }
        return {
          vaultAddress: vault.contract_evm_address.address,
          vaultDecimals: vault.contract_evm_address.metadata.erc20.decimals,
          underlyingDecimals: vault.underlying_evm_address.metadata.erc20.decimals,
          blockNumbers: vault.transfers.map((t) => t.blockNumber),
        };
      },
      "shareToUnderlyingRates",
    ),

    handleRpcErrors({ msg: "mapping vault share rate", data: { chain } }),

    // flatten the result
    Rx.mergeMap((vaults) => Rx.from(vaults)),
  );

  const govTransfersByVault$ = govTransferQueries$.pipe(
    // for gov vaults, we ignore the vault address and the associated maxi vault to avoid double counting
    Rx.map((vault) => ({
      ...vault,
      ignoreAddresses: [
        normalizeAddress("0x0000000000000000000000000000000000000000"),
        normalizeAddress(vault.contract_evm_address.address),
      ],
    })),

    // batch vault config by some reasonable amount that the RPC can handle
    Rx.bufferCount(200),

    mapErc20Transfers(
      provider,
      chain,
      (vault) => {
        if (!vault.underlying_evm_address.metadata.erc20) {
          throw new Error("Vault contract is not ERC20");
        }
        // for gov vaults we don't have a share token so we use the underlying token
        // transfers and filter on those transfer from and to the contract address
        return {
          address: vault.underlying_evm_address.address,
          decimals: vault.underlying_evm_address.metadata.erc20?.decimals,
          fromBlock: vault.fromBlock,
          toBlock: vault.toBlock,
          trackAddress: vault.contract_evm_address.address,
        };
      },
      "transfers",
    ),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping erc20Transfers", data: { chain } }),

    // flatten
    Rx.mergeMap((vaults) => Rx.from(vaults)),

    // make is so we are transfering from the user in and out the vault
    Rx.map((vault) => ({
      ...vault,
      transfers: vault.transfers.map(
        (transfer): TokenizedVaultUserTransfer => ({
          ...transfer,
          vaultAddress: vault.contract_evm_address.address,
          // amounts are reversed because we are sending token to the vault, but we then have a positive balance
          sharesBalanceDiff: transfer.sharesBalanceDiff.negated(),
        }),
      ),
    })),

    // we set the share rate to 1 for gov vaults
    Rx.map((vault) => ({ ...vault, shareToUnderlyingRates: vault.transfers.map(() => new Decimal(1)) })),
  );

  // join gov and non gov vaults
  const rawTransfersByVault$ = Rx.merge(standardTransfersByVault, govTransfersByVault$);

  const transfers$ = rawTransfersByVault$.pipe(
    // now move to transfers representation
    Rx.mergeMap((vault) =>
      vault.transfers.map((transfer, idx) => ({
        ...transfer,
        shareToUnderlyingRate: vault.shareToUnderlyingRates[idx],
        vault: { ...vault, transfers: undefined, shareToUnderlyingRates: undefined },
      })),
    ),

    // remove ignored addresses
    Rx.filter((transfer) => {
      const shouldIgnore = transfer.vault.ignoreAddresses.some(
        (ignoreAddr) => ignoreAddr === normalizeAddress(transfer.ownerAddress),
      );
      if (shouldIgnore) {
        logger.debug({ msg: "ignoring transfer", data: { chain, transfer } });
      }
      return !shouldIgnore;
    }),

    Rx.tap((userAction) =>
      logger.trace({
        msg: "processing user action",
        data: {
          chain,
          vault: userAction.vault.vault_key,
          transaction: userAction.transactionHash,
          ownerAddress: userAction.ownerAddress,
          vaultAddress: userAction.vaultAddress,
          amount: userAction.sharesBalanceDiff.toString(),
        },
      }),
    ),
  );

  const enhancedTransfers$ = transfers$.pipe(
    // batch transfer events before fetching additional infos
    Rx.bufferCount(500),

    // we need the balance of each owner
    mapERC20TokenBalance(
      chain,
      provider,
      (t) => ({
        blockNumber: t.blockNumber,
        decimals: t.sharesDecimals,
        contractAddress: t.vaultAddress,
        ownerAddress: t.ownerAddress,
      }),
      "ownerBalance",
    ),

    handleRpcErrors({ msg: "mapping owner balance", data: { chain } }),

    // we also need the date of each block
    mapBlockDatetime(provider, (t) => t.blockNumber, "blockDatetime"),

    // we want to catch any errors from the RPC
    handleRpcErrors({ msg: "mapping block datetimes", data: { chain } }),

    // flatten the resulting array
    Rx.mergeMap((transfers) => Rx.from(transfers)),
  );

  // insert relational data pipe
  const insertPipeline$ = enhancedTransfers$
    .pipe(
      // batch by an amount more suitable for batch inserts
      Rx.bufferCount(2000),

      Rx.tap((transfers) =>
        logger.debug({
          msg: "inserting transfer batch",
          data: { chain, count: transfers.length },
        }),
      ),

      // insert the owner addresses
      mapAddressToEvmAddressId(
        client,
        (transfer) => ({ chain, address: transfer.ownerAddress, metadata: {} }),
        "owner_evm_address_id",
      ),

      // fetch the vault addresses
      mapAddressToEvmAddressId(
        client,
        (transfer) => ({ chain, address: transfer.vaultAddress, metadata: {} }),
        "vault_evm_address_id",
      ),

      // insert the transactions if needed
      mapTransactionToEvmTransactionId(
        client,
        (transfer) => ({
          chain,
          hash: transfer.transactionHash,
          block_number: transfer.blockNumber,
          block_datetime: transfer.blockDatetime,
        }),
        "evm_transaction_id",
      ),
    )
    // insert the actual shares updates data
    .pipe(
      // insert to the shares transfer table
      Rx.mergeMap(async (transfers) => {
        // short circuit if there's nothing to do
        if (transfers.length === 0) {
          return [];
        }

        await db_query<{ beefy_vault_id: number }>(
          `INSERT INTO vault_shares_transfer_ts (
              datetime,
              chain,
              block_number,
              evm_transaction_id,
              owner_evm_address_id,
              vault_evm_address_id,
              shares_balance_diff,
              shares_balance_after
              ) VALUES %L
              ON CONFLICT (owner_evm_address_id, vault_evm_address_id, evm_transaction_id, datetime) 
              DO NOTHING`,
          [
            transfers.map((transfer) => [
              transfer.blockDatetime.toISOString(),
              transfer.chain,
              transfer.blockNumber,
              transfer.evm_transaction_id,
              transfer.owner_evm_address_id,
              transfer.vault_evm_address_id,
              transfer.sharesBalanceDiff.toString(),
              transfer.ownerBalance.toString(),
            ]),
          ],
          client,
        );
        return transfers;
      }),

      // insert to the vault shares rate
      Rx.mergeMap(async (transfers) => {
        // short circuit if there's nothing to do
        if (transfers.length === 0) {
          return [];
        }

        await db_query<{ beefy_vault_id: number }>(
          `INSERT INTO vault_to_underlying_rate_ts (
                datetime,
                block_number,
                chain,
                vault_evm_address_id,
                shares_to_underlying_rate
              ) VALUES %L
              ON CONFLICT (vault_evm_address_id, datetime) 
              DO NOTHING`,
          [
            transfers.map((transfer) => [
              transfer.blockDatetime.toISOString(),
              transfer.blockNumber,
              transfer.chain,
              transfer.vault_evm_address_id,
              transfer.shareToUnderlyingRate.toString(),
            ]),
          ],
          client,
        );
        return transfers;
      }),

      Rx.tap((transfers) =>
        logger.debug({ msg: "done processing chain batch", data: { chain, count: transfers.length } }),
      ),
    );

  return insertPipeline$;
}

function upsertVauts(client: PoolClient) {
  logger.info({ msg: "importing vaults" });

  const pipeline$ = Rx.of(...allChainIds).pipe(
    Rx.tap((chain) => logger.debug({ msg: "importing chain vaults", data: { chain } })),

    // fetch vaults from git file history
    Rx.mergeMap(getAllVaultsFromGitHistory, 1 /* concurrent = 1, we don't want concurrency here */),
    Rx.mergeMap((vaults) => Rx.from(vaults)), // flatten

    // batch by some reasonable amount
    Rx.bufferCount(200),

    Rx.tap((vaults) =>
      logger.info({ msg: "inserting vaults", data: { total: vaults.length, chain: vaults?.[0]?.chain } }),
    ),

    // map the contract address and underlying address to db ids
    mapAddressToEvmAddressId(
      client,
      (vault) => {
        // gov vault are not erc20 vaults
        if (vault.is_gov_vault) {
          return {
            chain: vault.chain,
            address: vault.contract_address,
            metadata: {},
          };
        } else {
          return {
            chain: vault.chain,
            address: vault.contract_address,
            metadata: { erc20: { name: vault.token_name, decimals: vault.token_decimals, price_feed_key: vault.id } },
          };
        }
      },
      "contract_evm_address_id",
    ),

    mapAddressToEvmAddressId(
      client,
      (vault) => ({
        chain: vault.chain,
        address: vault.want_address,
        metadata: {
          erc20: { name: null, decimals: vault.want_decimals, price_feed_key: vault.price_oracle.want_oracleId },
        },
      }),
      "underlying_evm_address_id",
    ),

    // insert to the vault table
    Rx.mergeMap(async (vaults) => {
      // short circuit if there's nothing to do
      if (vaults.length === 0) {
        return [];
      }

      const result = await db_query<{ beefy_vault_id: number }>(
        `INSERT INTO beefy_vault (
            vault_key,
            chain,
            contract_evm_address_id,
            underlying_evm_address_id,
            end_of_life,
            has_erc20_shares_token,
            assets_price_feed_keys
          ) VALUES %L
          ON CONFLICT (contract_evm_address_id) DO UPDATE 
            SET vault_key = EXCLUDED.vault_key, -- vault key can change, ppl add "-eol" when it's time to end
            contract_evm_address_id = EXCLUDED.contract_evm_address_id,
            underlying_evm_address_id = EXCLUDED.underlying_evm_address_id,
            end_of_life = EXCLUDED.end_of_life,
            assets_price_feed_keys = EXCLUDED.assets_price_feed_keys
          RETURNING vault_id`,
        [
          vaults.map((vault) => [
            vault.id,
            vault.chain,
            vault.contract_evm_address_id,
            vault.underlying_evm_address_id,
            vault.eol,
            !vault.is_gov_vault, // gov vaults don't have erc20 shares tokens
            strArrToPgStrArr(vault.price_oracle.assets),
          ]),
        ],
        client,
      );
      return result;
    }),

    Rx.tap({
      complete: () => {
        logger.info({ msg: "done vault configs data for all chains" });
      },
    }),
  );

  return consumeObservable(pipeline$);
}

function fetchPrices(client: PoolClient) {
  logger.info({ msg: "fetching vault prices" });

  const pipeline$ = vaultList$(client)
    .pipe(
      Rx.filter((vault) => vault.end_of_life === false), // only live vaults

      // extract the price feed keys we need to fetch prices for
      Rx.mergeMap((vault) => {
        return [
          vault.contract_evm_address.metadata.erc20?.price_feed_key || null,
          vault.underlying_evm_address.metadata.erc20?.price_feed_key || null,
          //getChainWNativeTokenOracleId(vault.chain),
          //...vault.assets_price_feed_keys,
        ].filter((k) => !!k) as string[];
      }),

      // remove duplicates
      Rx.distinct(),

      // make them into a query object
      Rx.map((price_feed_key) => ({ price_feed_key })),
    )
    .pipe(
      // work by batches to be kind on the database
      Rx.bufferCount(200),

      // find out which data is missing
      Rx.mergeMap(async (priceFeedQueries) => {
        const sortedPriceFeedQueries = sortBy(priceFeedQueries, "price_feed_key");
        const results = await db_query<{ price_feed_key: string; last_inserted_datetime: Date }>(
          `SELECT 
            price_feed_key,
            last(datetime, datetime) as last_inserted_datetime
          FROM asset_price_ts 
          WHERE price_feed_key IN (%L)
          GROUP BY price_feed_key`,
          [sortedPriceFeedQueries.map((q) => q.price_feed_key)],
          client,
        );
        const resultsMap = keyBy(results, "price_feed_key");

        // if we have data already, we want to only fetch new data
        // otherwise, we aim for the last 24h of data
        let fromDate = new Date(new Date().getTime() - 1000 * 60 * 60 * 24);
        let toDate = new Date();
        return priceFeedQueries.map((q) => {
          if (resultsMap[q.price_feed_key]?.last_inserted_datetime) {
            fromDate = resultsMap[q.price_feed_key].last_inserted_datetime;
          }
          return {
            ...q,
            fromDate,
            toDate,
          };
        });
      }),

      // ok, flatten all price feed queries
      Rx.mergeMap((priceFeedQueries) => Rx.from(priceFeedQueries)),
    )
    .pipe(
      // now we fetch
      Rx.mergeMap(async (priceFeedQuery) => {
        logger.debug({ msg: "fetching prices", data: priceFeedQuery });
        const prices = await fetchBeefyPrices("15min", priceFeedQuery.price_feed_key, {
          startDate: priceFeedQuery.fromDate,
          endDate: priceFeedQuery.toDate,
        });
        logger.debug({ msg: "got prices", data: { priceFeedQuery, prices: prices.length } });
        return prices;
      }, 10 /* concurrency */),

      Rx.catchError((err) => {
        logger.error({ msg: "error fetching prices", err });
        logger.error(err);
        return Rx.EMPTY;
      }),

      // flatten the array of prices
      Rx.mergeMap((prices) => Rx.from(prices)),
    )
    .pipe(
      // batch by some reasonable amount for the database
      Rx.bufferCount(5000),

      // insert into the prices table
      Rx.mergeMap(async (prices) => {
        // short circuit if there's nothing to do
        if (prices.length === 0) {
          return [];
        }

        logger.debug({ msg: "inserting prices", data: { count: prices.length } });

        await db_query<{}>(
          `INSERT INTO asset_price_ts (
            datetime,
            price_feed_key,
            usd_value
          ) VALUES %L
          ON CONFLICT (price_feed_key, datetime) DO NOTHING`,
          [prices.map((price) => [price.datetime, price.oracleId, price.value])],
          client,
        );
        return prices;
      }),
    );

  return consumeObservable(pipeline$).then(() => logger.info({ msg: "done fetching prices" }));
}

function handleRpcErrors<TInput>(logInfos: { msg: string; data: object }): Rx.OperatorFunction<TInput, TInput> {
  return ($source) =>
    $source.pipe(
      // retry some errors
      retryRpcErrors(logInfos),

      // or catch the rest
      Rx.catchError((error) => {
        // don't show the full stack trace if we know what's going on
        if (isErrorDueToMissingDataFromNode(error)) {
          logger.error({ msg: `need archive node for ${logInfos.msg}`, data: logInfos.data });
        } else {
          logger.error({ msg: "error mapping token balance", data: { ...logInfos, error } });
          logger.error(error);
        }
        return Rx.EMPTY;
      }),
    );
}
