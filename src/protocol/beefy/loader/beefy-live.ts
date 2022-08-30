import { zipWith } from "lodash";
import { runMain } from "../../../utils/process";
import Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { strAddressToPgBytea, withPgClient } from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";
import { PoolClient } from "pg";

async function main(client: PoolClient) {
  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  const chainListObs = new Rx.Observable<Chain>();

  const pipeline = chainListObs.pipe(
    // fetch vaults from git file history
    Rx.mergeMap(getAllVaultsFromGitHistory),

    // batch by some reasonable amount
    Rx.windowCount(500),
    Rx.mergeAll(),

    // map the contract address and underlying address to db ids
    Rx.map(async (vaults) => {
      const result = await client.query(
        `INSERT INTO evm_address (chain, address, metadata) VALUES %L 
          ON CONFLICT DO UPDATE SET metadata = EXCLUDED.metadata
          RETURNING evm_address_id`,
        vaults.map((vault) => [
          vault.chain,
          strAddressToPgBytea(vault.token_address),
          { erc20: { name: vault.token_name, decimals: vault.token_decimals, price_feed_id: vault.id } },
        ])
      );
      return zipWith(vaults, result.rows, (vault, row) => ({ contract_evm_address_id: row.id, ...vault }));
    }),
    Rx.mergeAll(),

    Rx.map(async (vaults) => {
      const result = await client.query(
        `INSERT INTO evm_address (chain, address, metadata) VALUES %L 
          ON CONFLICT DO UPDATE SET metadata = EXCLUDED.metadata
          RETURNING evm_address_id`,
        vaults.map((vault) => [
          vault.chain,
          strAddressToPgBytea(vault.want_address),
          { erc20: { name: null, decimals: vault.token_decimals, price_feed_id: vault.price_oracle.want_oracleId } },
        ])
      );
      return zipWith(vaults, result.rows, (vault, row) => ({ underlying_evm_address_id: row.id, ...vault }));
    }),
    Rx.mergeAll(),

    // insert to the vault table
    Rx.map(async (vaults) => {
      const result = await client.query(
        `INSERT INTO beefy_vault (
            vault_key,
            contract_evm_address_id,
            underlying_evm_address_id,
            end_of_life,
            has_erc20_shares_token,
            assets_price_feed_keys
          ) VALUES %L 
          ON CONFLICT DO UPDATE 
            SET vault_key = EXCLUDED.vault_key, -- vault key can change, ppl add "-eol" when it's time to end
            contract_evm_address_id = EXCLUDED.contract_evm_address_id,
            underlying_evm_address_id = EXCLUDED.underlying_evm_address_id,
            end_of_life = EXCLUDED.end_of_life,
            assets_price_feed_keys = EXCLUDED.assets_price_feed_keys
          RETURNING beefy_vault_id`,
        vaults.map((vault) => [
          vault.id,
          vault.contract_evm_address_id,
          vault.underlying_evm_address_id,
          vault.eol,
          !vault.is_gov_vault, // gov vaults don't have erc20 shares tokens
          vault.price_oracle.assets,
        ])
      );
      return result.rows;
    }),
    Rx.mergeAll()
  );
}

runMain(withPgClient(main));
