import { Chain } from "../../types/chain";
import { DbClient, db_query } from "../../utils/db";
import { AsyncCache } from "./cache";

export class BlockService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getBlockAroundADate(chain: Chain, datetime: Date) {
    const isoDatetime = datetime.toISOString();
    return db_query<{
      datetime: Date;
      diff: string;
      chain: Chain;
      block_number: number;
    }>(
      `
      SELECT
        *
      FROM (
        (
          SELECT 
            datetime, 
            EXTRACT(EPOCH FROM (datetime - %L::timestamptz))::integer as diff_sec,
            chain,
            block_number 
          FROM block_ts
          WHERE chain = %L 
            and datetime between %L::timestamptz - '1 day'::interval and %L::timestamptz + '1 day'::interval 
            and datetime <= %L 
          ORDER BY datetime DESC 
          LIMIT 10
        ) 
          UNION ALL
        (
          SELECT 
            datetime, 
            EXTRACT(EPOCH FROM (datetime - %L::timestamptz))::integer as diff_sec,
            chain,
            block_number 
          FROM block_ts
          WHERE chain = %L 
            and datetime between %L::timestamptz - '1 day'::interval and %L::timestamptz + '1 day'::interval 
            and datetime >= %L 
          ORDER BY datetime DESC 
          LIMIT 10
        )
      ) as t
      ORDER BY abs(diff_sec) ASC
      `,
      [isoDatetime, chain, isoDatetime, isoDatetime, isoDatetime, isoDatetime, chain, isoDatetime, isoDatetime, isoDatetime],
      this.services.db,
    );
  }
}
