import { Chain } from "../../types/chain";
import { SamplingPeriod } from "../../types/sampling";
import { DbClient, db_query } from "../../utils/db";
import { AsyncCache } from "./cache";

export class BlockService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  async getBlockAroundADate(chain: Chain, datetime: Date, lookAround: SamplingPeriod, halfLimit: number) {
    const isoDatetime = datetime.toISOString();
    const queryParams = [isoDatetime, chain, isoDatetime, lookAround, isoDatetime, lookAround, isoDatetime, halfLimit];
    return db_query<{
      datetime: Date;
      diff_sec: number;
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
            and datetime between %L::timestamptz - %L::interval and %L::timestamptz + %L::interval 
            and datetime <= %L 
          ORDER BY datetime DESC 
          LIMIT %L
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
            and datetime between %L::timestamptz - %L::interval and %L::timestamptz + %L::interval 
            and datetime >= %L 
          ORDER BY datetime DESC 
          LIMIT %L
        )
      ) as t
      ORDER BY abs(diff_sec) ASC
      `,
      [...queryParams, ...queryParams],
      this.services.db,
    );
  }
}
