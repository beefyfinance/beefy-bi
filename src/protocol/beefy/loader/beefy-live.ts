import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_query, mapAddressToEvmAddressId, strArrToPgStrArr, withPgClient } from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger2";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { flatten, sample } from "lodash";
import { RPC_URLS, WS_RPC_URLS } from "../../../utils/config";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";

const logger = rootLogger.child({ module: "import-script", component: "beefy-live" });

async function main() {
  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  await withPgClient(subsribeToSharesAmountChanges)();
  //await withPgClient(upsertVauts)();
}

runMain(main);

function subsribeToSharesAmountChanges(client: PoolClient) {
  const providers: Record<string, ethers.providers.JsonRpcProvider> = {};
  const getProvider = (chain: Chain) => {
    if (providers[chain] === undefined) {
      const rpcUrl = sample(RPC_URLS[chain]) as string;
      providers[chain] = new ethers.providers.JsonRpcProvider(rpcUrl);
    }
    return providers[chain];
  };

  // every now and then
  const pipeline$ = Rx.interval(/*2 * 60 * */ 5 * 1_000).pipe(
    // start immediately, otherwise we have to wait for the interval to start
    Rx.startWith(-1),

    Rx.tap((i) => logger.info({ msg: "subscribing to shares amount changes", data: { i } })),

    // start a new observable for each chain
    Rx.map(() =>
      Rx.from(allChainIds).pipe(
        Rx.mergeMap(async (chain) => {
          const provider = getProvider(chain);
          try {
            const blockNumber = await provider.getBlockNumber();
            return { chain, blockNumber };
          } catch (error) {
            logger.error({ msg: "error getting block number", data: { chain, error } });
            logger.trace(error);
          }
          return { chain, blockNumber: null };
        }),

        Rx.tap(({ blockNumber, chain }) =>
          logger.info({ msg: "subscribing to shares amount changes", data: { blockNumber, chain } }),
        ),
      ),
    ),

    Rx.tap((chain) => logger.info({ msg: "subscribing to shares amount changes", data: { chain } })),

    // flatten the results
    Rx.mergeAll(),
  );
  /*
  const pipeline$ = vaultListUpdates$(client).pipe(
    //Rx.take(50), // debug
    //Rx.filter((v) => v.chain === "fantom"), //debug

    // group by chain
    Rx.groupBy((v) => v.chain),

    // consume all the vaults for this chain
    Rx.mergeMap((group$) =>
      group$.pipe(
        // get 500 contracts or wait some time
        Rx.bufferTime(5_000, 5_000, 100),

        // only get groups with a size, as bufferTime can produce empty groups
        Rx.filter((vaults) => vaults.length > 0),

        // for each batch of contracts, register an event handler
        Rx.mergeMap((vaults) => {
          return new Rx.Observable<{ event: any; chain: Chain }>((subscriber) => {
            const provider = getProvider(group$.key);
            provider.getBlockNumber();

            const topics = flatten(
              vaults.map((v) => {
                const contract = new ethers.Contract(v.contract_evm_address.address, ERC20Abi, provider);
                const eventFilter = contract.filters.Transfer(null, contract.address);
                return eventFilter.topics as string[];
              })
            );

            logger.info({
              msg: "Registering wss connection",
              data: { chain: group$.key, total: topics.length, topics },
            });
            provider.on({ topics }, (event) => {
              logger.info({ msg: "event handler", data: { event } });
              subscriber.next({ chain: group$.key, event });
              throw new Error("HERE");
            });
          });
        }),

        Rx.tap((v) => logger.info({ msg: "event", data: v }))
      )
    ),

    Rx.tap((event) => logger.info({ msg: "Got event", data: { event } })),

    // batch all events
    Rx.bufferTime(5_000, 5_000, 500),
    // only get groups with a size, as bufferTime can produce empty groups
    Rx.filter((events) => events.length > 0),

    // push to database
    Rx.mergeMap((events) => {
      logger.info({ msg: "Got event batch", data: { events } });
      return Rx.of(events.length);
    })
  );*/
  return consumeObservable(pipeline$);
}

function upsertVauts(client: PoolClient) {
  const pipeline$ = Rx.of(...allChainIds).pipe(
    Rx.tap((chain) => logger.info({ msg: "importing chain", data: { chain } })),

    // fetch vaults from git file history
    Rx.mergeMap(getAllVaultsFromGitHistory, 1 /* concurrent = 1, we don't want concurrency here */),
    Rx.mergeMap((vaults) => Rx.from(vaults)), // flatten

    // batch by some reasonable amount
    Rx.bufferCount(200),

    Rx.tap((vaults) =>
      logger.info({ msg: "inserting vaults", data: { total: vaults.length, chain: vaults?.[0]?.chain } }),
    ),

    // map the contract address and underlying address to db ids
    Rx.mergeMap((vaults) =>
      mapAddressToEvmAddressId(
        client,
        vaults,
        (vault) => ({
          chain: vault.chain,
          address: vault.token_address,
          metadata: { erc20: { name: vault.token_name, decimals: vault.token_decimals, price_feed_key: vault.id } },
        }),
        "contract_evm_address_id",
      ),
    ),

    Rx.mergeMap((vaults) =>
      mapAddressToEvmAddressId(
        client,
        vaults,
        (vault) => ({
          chain: vault.chain,
          address: vault.want_address,
          metadata: {
            erc20: { name: null, decimals: vault.want_decimals, price_feed_key: vault.price_oracle.want_oracleId },
          },
        }),
        "underlying_evm_address_id",
      ),
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
  );

  return consumeObservable(pipeline$);
}
