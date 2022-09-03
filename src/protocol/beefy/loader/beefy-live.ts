import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_query, mapAddressToEvmAddressId, strArrToPgStrArr, vaultList$, withPgClient } from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger2";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { sample } from "lodash";
import { samplingPeriodMs } from "../../../types/sampling";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MS_PER_BLOCK_ESTIMATE, RPC_URLS } from "../../../utils/config";
import { fetchBeefyVaultV6Transfers } from "../connector/vault-transfers";
import { transferEventToDb } from "../../common/loader/transfer-event-to-db";

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
  const pollLiveData = withPgClient(fetchLatestData);
  const pollVaultData = withPgClient(upsertVauts);

  return new Promise(async () => {
    // start polling live data immediately
    //await pollVaultData();
    await pollLiveData();
    // then poll every now and then
    setInterval(pollLiveData, samplingPeriodMs["15min"]);
    setInterval(pollVaultData, samplingPeriodMs["1day"]);
  });
}

runMain(main);

function fetchLatestData(client: PoolClient) {
  const providers: Record<string, ethers.providers.JsonRpcProvider> = {};
  const getProvider = (chain: Chain) => {
    if (providers[chain] === undefined) {
      const rpcUrl = sample(RPC_URLS[chain]) as string;
      providers[chain] = new ethers.providers.JsonRpcProvider(rpcUrl);
    }
    return providers[chain];
  };

  const pipeline$ = vaultList$(client)
    // define the scope of our pipeline
    .pipe(
      //Rx.filter((vault) => vault.chain === "celo"), //debug

      Rx.filter((vault) => vault.end_of_life === false), // only live vaults
      Rx.filter((vault) => vault.has_erc20_shares_token), // only vaults with a shares token
      Rx.filter((vault) => !!vault.contract_evm_address.metadata.erc20), // only vaults with a shares token
    )
    .pipe(
      // create a different sub-pipeline for each chain
      Rx.groupBy((vault) => vault.chain),

      Rx.tap((chainVaults$) => logger.debug({ msg: "processing chain", data: { chain: chainVaults$.key } })),

      // process each chain separately
      Rx.mergeMap((chainVaults$) =>
        chainVaults$
          // connector data pipeline
          .pipe(
            // batch vault config by some reasonable amount that the RPC can handle
            Rx.bufferCount(200),

            // go get the latest block number for this chain
            Rx.mergeMap(async (vaults) => [vaults, await getProvider(chainVaults$.key).getBlockNumber()] as const),

            // call our connector to get the transfers
            Rx.mergeMap(([vaults, latestBlockNumber]) => {
              // fetch the last hour of data
              const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[chainVaults$.key];
              const period = samplingPeriodMs["1hour"];
              const periodInBlockCountEstimate = Math.floor(period / MS_PER_BLOCK_ESTIMATE[chainVaults$.key]);

              const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate);

              return fetchBeefyVaultV6Transfers(
                getProvider(chainVaults$.key),
                chainVaults$.key,
                vaults.map((vault) => {
                  if (!vault.contract_evm_address.metadata.erc20) {
                    throw new Error("no decimals");
                  }
                  return {
                    address: vault.contract_evm_address.address,
                    decimals: vault.contract_evm_address.metadata.erc20?.decimals,
                  };
                }),
                latestBlockNumber - blockCountToFetch,
                latestBlockNumber,
              );
            }),

            // we want to catch any errors from the RPC
            Rx.catchError((error) => {
              logger.error({ msg: "error importing latest chain data", data: { chain: chainVaults$.key, error } });
              logger.error(error);
              return Rx.EMPTY;
            }),

            // flatten the resulting array
            Rx.mergeMap((transfers) => Rx.from(transfers)),

            // send to the db write pipeline
            transferEventToDb(client, chainVaults$.key, getProvider(chainVaults$.key)),
          ),
      ),
    );
  return consumeObservable(pipeline$).then(() => logger.info({ msg: "done importing live data for all chains" }));
}

function upsertVauts(client: PoolClient) {
  const pipeline$ = Rx.of(...allChainIds).pipe(
    Rx.tap((chain) => logger.info({ msg: "importing chain vaults", data: { chain } })),

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

  return consumeObservable(pipeline$).then(() => logger.info({ msg: "done vault configs data for all chains" }));
}
