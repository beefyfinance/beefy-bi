import { DbClient, db_query_one } from "../../utils/db";
import { AsyncCache } from "./cache";

export class InvestorService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getInvestorId(address: string) {
    const cacheKey = `api:investor-service:${address.toLocaleLowerCase()}`;
    const ttl = 1000 * 60 * 60 * 24 * 7; // 1 week
    return this.services.cache.wrap(cacheKey, ttl, async () => {
      const res = await db_query_one<{ investor_id: number }>(
        ` SELECT investor_id 
        FROM investor 
        WHERE address = hexstr_to_bytea(%L)`,
        [address],
        this.services.db,
      );
      return res?.investor_id || null;
    });
  }
}
