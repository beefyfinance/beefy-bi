import { DbImportState, ImportStateTypes, hydrateImportStateRangesFromDb } from "../../protocol/common/loader/import-state";
import { Chain } from "../../types/chain";
import { DbClient, db_query_one } from "../../utils/db";
import { AsyncCache } from "./cache";

export class ImportStateService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getImportStateByKey(importStateKey: string) {
    const cacheKey = `api:import-state:${importStateKey.toLocaleLowerCase()}`;
    const ttl = 1000 * 60 * 5; // 5 min ttl
    const res = await this.services.cache.wrap(cacheKey, ttl, async () =>
      db_query_one<DbImportState>(
        `
        SELECT 
          import_key as "importKey",
          import_data as "importData"
        FROM import_state
        WHERE import_key = %L
        LIMIT 1`,
        [importStateKey],
        this.services.db,
      ),
    );
    if (res !== null) {
      hydrateImportStateRangesFromDb(res);
    }
    return res || null;
  }

  async getChainLatestIndexedBlock(chain: Chain, importStateType: ImportStateTypes) {
    const res = await db_query_one<{ latest_indexed_block: number }>(
      `
        select max((import_data->>'chainLatestBlockNumber')::integer) as latest_indexed_block
        from import_state 
        where import_data->>'chain' = %L
        and import_data->>'type' = %L
      `,
      [chain, importStateType],
      this.services.db,
    );
    return res?.latest_indexed_block || null;
  }

  async getChainHasAnythingToRetry(chain: Chain, importStateType: ImportStateTypes) {
    const res = await db_query_one<{ has_anything_to_retry: boolean }>(
      `
        select bool_or(jsonb_array_length(import_data->'ranges'->'toRetry')::boolean) as has_anything_to_retry
        from import_state
        where import_data->>'chain' = %L
        and import_data->>'type' = %L
      `,
      [chain, importStateType],
      this.services.db,
    );
    return res?.has_anything_to_retry || false;
  }
}
