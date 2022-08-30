import { keyBy, uniqBy, zipWith } from "lodash";
import { runMain } from "../../../utils/process";
import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { db_query, strAddressToPgBytea, strArrToPgStrArr, withPgClient } from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";
import { PoolClient } from "pg";
import { rootLogger } from "../../../utils/logger2";

const logger = rootLogger.child({ module: "import-script", component: "beefy-live" });

async function main(client: PoolClient) {
  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  const pipeline = Rx.of(...allChainIds).pipe(
    Rx.tap((chain) => logger.info({ msg: "importing chain", data: { chain } })),

    // fetch vaults from git file history
    Rx.mergeMap(getAllVaultsFromGitHistory, 1 /* concurrent = 1, we don't want concurrency here */),

    // batch by some reasonable amount
    Rx.windowCount(500),
    Rx.mergeAll(),

    Rx.tap((vaults) =>
      logger.info({ msg: "inserting vaults", data: { total: vaults.length, chain: vaults?.[0]?.chain } })
    ),

    // map the contract address and underlying address to db ids
    Rx.mergeMap((vaults) =>
      mapEvmAddressId(
        client,
        vaults,
        (vault) => ({
          chain: vault.chain,
          address: vault.token_address,
          metadata: { erc20: { name: vault.token_name, decimals: vault.token_decimals, price_feed_id: vault.id } },
        }),
        "contract_evm_address_id"
      )
    ),

    Rx.mergeMap((vaults) =>
      mapEvmAddressId(
        client,
        vaults,
        (vault) => ({
          chain: vault.chain,
          address: vault.want_address,
          metadata: {
            erc20: { name: null, decimals: vault.want_decimals, price_feed_id: vault.price_oracle.want_oracleId },
          },
        }),
        "underlying_evm_address_id"
      )
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
            vault.contract_evm_address_id,
            vault.underlying_evm_address_id,
            vault.eol,
            !vault.is_gov_vault, // gov vaults don't have erc20 shares tokens
            strArrToPgStrArr(vault.price_oracle.assets),
          ]),
        ],
        client
      );
      return result;
    })
  );

  return consumeObservable(pipeline);
}

runMain(withPgClient(main));

function consumeObservable<TRes>(observable: Rx.Observable<TRes>) {
  return new Promise<TRes | null>((resolve, reject) => {
    let lastValue: TRes | null = null;
    observable.subscribe({
      error: reject,
      next: (value) => (lastValue = value),
      complete: () => resolve(lastValue),
    });
  });
}

// upsert the address of all objects and return the id in the specified field
async function mapEvmAddressId<TObj, TKey extends string>(
  client: PoolClient,
  objs: TObj[],
  getAddrData: (obj: TObj) => { chain: Chain; address: string; metadata: object },
  toKey: TKey
): Promise<(TObj & { [key in TKey]: number })[]> {
  // short circuit if there's nothing to do
  if (objs.length === 0) {
    return [];
  }

  const addressesToInsert = objs.map(getAddrData);
  const uniqueAddresses = uniqBy(addressesToInsert, ({ chain, address }) => `${chain}-${address}`);

  const result = await db_query<{ evm_address_id: number }>(
    `INSERT INTO evm_address (chain, address, metadata) VALUES %L
      ON CONFLICT (chain, address) DO UPDATE SET metadata = EXCLUDED.metadata
      RETURNING evm_address_id`,
    [uniqueAddresses.map((addr) => [addr.chain, strAddressToPgBytea(addr.address), addr.metadata])],
    client
  );
  const addressIdMap = keyBy(
    zipWith(uniqueAddresses, result, (addr, row) => ({ addr, evm_address_id: row.evm_address_id })),
    (res) => `${res.addr.chain}-${res.addr.address}`
  );

  const objsWithId = zipWith(
    objs,
    addressesToInsert,
    (obj, addr) =>
      ({
        ...obj,
        [toKey]: addressIdMap[`${addr.chain}-${addr.address}`].evm_address_id,
      } as TObj & { [key in TKey]: number })
  );

  return objsWithId;
}
