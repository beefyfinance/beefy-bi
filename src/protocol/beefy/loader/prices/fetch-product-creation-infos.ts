import { keyBy } from "lodash";
import { PoolClient } from "pg";
import * as Rx from "rxjs";
import { Chain } from "../../../../types/chain";
import { BATCH_DB_SELECT_SIZE, BATCH_MAX_WAIT_MS } from "../../../../utils/config";
import { db_query } from "../../../../utils/db";

export function fetchProductContractCreationInfos<TObj, TRes>(options: {
  client: PoolClient;
  getPriceFeedId: (obj: TObj) => number;
  formatOutput: (
    obj: TObj,
    contractCreationInfos: { chain: Chain; productId: number; contractCreatedAtBlock: number; contractCreationDate: Date } | null,
  ) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.bufferTime(BATCH_MAX_WAIT_MS, undefined, BATCH_DB_SELECT_SIZE),

    // upsert data and map to input objects
    Rx.mergeMap(async (objs) => {
      // short circuit if there's nothing to do
      if (objs.length === 0) {
        return [];
      }

      const objAndData = objs.map((obj) => ({ obj, priceFeedId: options.getPriceFeedId(obj) }));

      type TRes = { priceFeedId: number; productId: number; chain: Chain; contractCreatedAtBlock: number; contractCreationDate: Date };
      const results = await db_query<TRes>(
        `SELECT 
              p.price_feed_2_id as "priceFeedId",
              p.product_id as "productId",
              p.chain as "chain",
              (import_data->'contractCreatedAtBlock')::integer as "contractCreatedAtBlock",
              (import_data->>'contractCreationDate')::timestamptz as "contractCreationDate"
          FROM import_state i
            JOIN product p on p.product_id = (i.import_data->'productId')::integer
          WHERE price_feed_2_id IN (%L)`,
        [objAndData.map((obj) => obj.priceFeedId)],
        options.client,
      );

      // ensure results are in the same order as the params
      const idMap = keyBy(
        results.map((res) => {
          res.contractCreationDate = new Date(res.contractCreationDate);
          return res;
        }),
        "priceFeedId",
      );
      return objAndData.map((obj) => options.formatOutput(obj.obj, idMap[obj.priceFeedId] ?? null));
    }),

    // flatten objects
    Rx.concatMap((objs) => Rx.from(objs)),
  );
}
