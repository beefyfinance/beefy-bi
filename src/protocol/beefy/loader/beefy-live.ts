import { PoolClient } from "pg";
import Rx from "rxjs";
import { Chain } from "../../../types/chain";
import { getPgPool } from "../../../utils/db";
import { getAllVaultsFromGitHistory } from "../connector/vault-list";

export async function run() {
  // ---
  // from time to time, refresh vaults
  //
  // ---
  // create the insert pipeline for transfers
  // register logs for all contracts
  // emit to the pipeline on new log
  //

  const pgPool = await getPgPool();
  const client = await pgPool.connect();
  try {
    const chainListObs = new Rx.Observable<Chain>();

    const pipeline = chainListObs.pipe(
      // fetch vaults from git file history
      Rx.mergeMap(getAllVaultsFromGitHistory),

      // batch by some reasonable amount
      Rx.windowCount(500),

      // map the contract and underlying address to db ids
      Rx.map((vaults) => {})
    );
  } finally {
    client.release();
  }
}

async function upsertAddresses(client: PoolClient, addresses chain: Chain, address: string, metadata: object): number {
  const result = await client.query(
    `
    INSERT INTO evm_address (chain, address, metadata) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`,
    [address]
  );
  return result.rows[0].id;
}
