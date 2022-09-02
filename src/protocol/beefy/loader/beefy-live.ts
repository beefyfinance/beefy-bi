import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_query, mapAddressToEvmAddressId, strArrToPgStrArr, vaultList$, withPgClient } from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger2";
import { consumeObservable } from "../../../utils/observable";
import { ethers } from "ethers";
import { flatten, sample } from "lodash";
import { RPC_URLS, WS_RPC_URLS } from "../../../utils/config";
import ERC20Abi from "../../../../data/interfaces/standard/ERC20.json";
import { fetchBeefyVaultV6Transfers } from "../connector/vault-transfers";
import { TokenizedVaultUserAction } from "../../types/connector";
import { mapERC20TokenBalance } from "../../common/connector/owner-balance";
import { mapBlockDatetime } from "../../common/connector/block-datetime";

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

  await withPgClient(fetchLatestData)();
  //await withPgClient(upsertVauts)();
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

  const pipeline$ = vaultList$(client).pipe(
    Rx.filter((vault) => vault.chain === "metis"), //debug

    Rx.groupBy((vault) => vault.chain),

    Rx.tap((chainVaults$) => logger.info({ msg: "processing chain", data: { chain: chainVaults$.key } })),

    // process each chain separately
    Rx.mergeMap((chainVaults$) =>
      chainVaults$.pipe(
        // batch vault config by some reasonable amount that the RPC can handle
        Rx.bufferCount(200),

        // go get the latest block  for this chain
        Rx.mergeMap(async (vaults) => [vaults, await getProvider(chainVaults$.key).getBlock("latest")] as const),

        // call our connector to get the transfers
        Rx.mergeMap(([vaults, latestBlock]) =>
          fetchBeefyVaultV6Transfers(
            getProvider(chainVaults$.key),
            chainVaults$.key,
            vaults.map((vault) => vault.contract_evm_address.address),
            latestBlock.number - 100,
            latestBlock.number,
          ),
        ),

        // flatten the resulting array
        Rx.mergeMap((transfers) => Rx.from(transfers)),

        Rx.tap((userAction) => logger.info({ msg: "processing user action", data: { userAction } })),

        // batch transfer events before fetching additional infos
        Rx.bufferCount(200),

        // we need the balance of each owner
        Rx.mergeMap((transfers) =>
          mapERC20TokenBalance(
            getProvider(chainVaults$.key),
            transfers,
            (t) => ({
              blockNumber: t.blockNumber,
              contractAddress: t.vaultAddress,
              ownerAddress: t.ownerAddress,
            }),
            "ownerBalance",
          ),
        ),

        // we also need the date of each block
        Rx.mergeMap((transfers) =>
          mapBlockDatetime(getProvider(chainVaults$.key), transfers, (t) => t.blockNumber, "blockDatetime"),
        ),

        // we want to catch any errors from the RPC
        Rx.catchError((error) => {
          logger.error({ msg: "error importing latest chain data", data: { chain: chainVaults$.key, error } });
          return Rx.EMPTY;
        }),

        // flatten the resulting array
        Rx.mergeMap((transfers) => Rx.from(transfers)),

        Rx.tap((transfers) =>
          logger.info({ msg: "inserting transfer batch", data: { chain: chainVaults$.key, count: transfers.length } }),
        ),

        // batch by an amount more suitable for batch inserts
        Rx.bufferCount(2000),

        // insert the addresses
        Rx.mergeMap((transfers) =>
          mapAddressToEvmAddressId(
            client,
            transfers,
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
      ),
    ),

    Rx.tap((event) => logger.info({ msg: "processing event", data: { event } })),
  );
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
