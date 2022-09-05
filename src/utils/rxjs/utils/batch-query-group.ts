import * as Rx from "rxjs";
import { zipWith } from "lodash";
import { ProgrammerError } from "./programmer-error";

/**
 * Often, we need to take a bunch of objects, make a bunch of query from them, and add the results to the objects.
 * We also want to avoid duplicate queries so we first need to extract unique queries from the objects,
 * then make the query batch and re-map the results to the initial objects.
 *
 * Examples:
 *   - make RPC calls from a list of events
 *   - insert some data in the DB from a list of events
 */
export function batchQueryGroup<TInputObj, TQueryObj, TResp, TKey extends string>(
  toQueryObj: (obj: TInputObj[]) => TQueryObj,
  getKey: (obj: TInputObj) => string | number,
  process: (queryObjs: TQueryObj[]) => Promise<TResp[]>,
  toKey: TKey,
): Rx.OperatorFunction<TInputObj[], (TInputObj & { [key in TKey]: TResp })[]> {
  return (objs$) =>
    objs$.pipe(
      Rx.mergeMap((objs) => {
        const pipeline$ = Rx.from(objs)
          // extract query objects by key
          .pipe(
            Rx.groupBy(getKey),
            Rx.mergeMap((group$) => group$.pipe(Rx.toArray())),
            Rx.map((objs) => ({ query: toQueryObj(objs), objs })),
            Rx.toArray(),
          )
          // make a batch query
          .pipe(
            Rx.mergeMap(async (queries) => {
              // assuming the process function returns the results in the same order as the input
              const results = await process(queries.map((q) => q.query));
              if (results.length !== queries.length) {
                throw new ProgrammerError({ msg: "Query and result length mismatch", queries, results });
              }
              return zipWith(queries, results, (q, r) => ({ ...q, result: r }));
            }),
            Rx.mergeAll(),
          )
          // re-emit all input objects with the corresponding result
          .pipe(
            Rx.map((resp) =>
              resp.objs.map((obj) => ({ ...obj, [toKey]: resp.result } as TInputObj & { [key in TKey]: TResp })),
            ),
            Rx.mergeAll(),
            Rx.toArray(),
          );
        return pipeline$;
      }),
    );
}
