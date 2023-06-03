import { Chain } from "../../types/chain";
import { SamplingPeriod } from "../../types/sampling";
import { DbClient, db_query } from "../../utils/db";
import { AsyncCache } from "./cache";

export class BlockService {
  constructor(private services: { db: DbClient; cache: AsyncCache }) {}

  public static blocksAroundADateResponseSchema = {
    description: "List of 2*`half_limit` blocks closest to the `utc_datetime`, for the given `chain` and `look_around` period.",
    type: "array",
    items: {
      type: "object",
      properties: {
        datetime: { type: "string", format: "date-time", description: "Block datetime, provided by the RPC" },
        diff_sec: { type: "number", description: "Difference between the requested `utc_datetime` and block `datetime`" },
        block_number: { type: "number" },
      },
      required: ["datetime", "diff_sec", "chain", "block_number"],
    },
  };

  async getBlockAroundADate(chain: Chain, datetime: Date, lookAround: SamplingPeriod, halfLimit: number) {
    const isoDatetime = datetime.toISOString();
    const queryParams = [isoDatetime, chain, isoDatetime, lookAround, isoDatetime, lookAround, isoDatetime, halfLimit];
    return db_query<{
      datetime: Date;
      diff_sec: number;
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
            block_number 
          FROM block_ts
          WHERE chain = %L 
            and datetime between %L::timestamptz - %L::interval and %L::timestamptz + %L::interval 
            and datetime > %L 
          ORDER BY datetime ASC 
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
