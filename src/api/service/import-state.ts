import { DbImportState, hydrateImportStateRangesFromDb } from "../../protocol/common/loader/import-state";
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
}
