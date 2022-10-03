import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";

const logger = rootLogger.child({ module: "loader", component: "by-chain" });

export function loaderByChain$<TInput extends { chain: Chain }, TOutput>(
  processChain: (chain: Chain) => (input$: Rx.Observable<TInput>) => Rx.Observable<TOutput>,
): Rx.OperatorFunction<TInput, TOutput> {
  type ImportState = { inProgress: boolean; start: Date };
  const importStates: Record<Chain, ImportState> = allChainIds.reduce(
    (acc, chain) => Object.assign(acc, { [chain]: { inProgress: false, start: new Date() } }),
    {} as Record<Chain, ImportState>,
  );
  let index = 0;

  const processorByChain = allChainIds.reduce(
    (acc, chain) => Object.assign(acc, { [chain]: processChain(chain) }),
    {} as Record<Chain, (input$: Rx.Observable<TInput>) => Rx.Observable<TOutput>>,
  );

  return Rx.pipe(
    // create a different sub-pipeline for each chain
    Rx.groupBy((product) => product.chain),

    // process each chain separately
    Rx.map((chainProducts$) => {
      logger.debug({ msg: "processing chain", data: { chain: chainProducts$.key } });

      if (importStates[chainProducts$.key].inProgress) {
        logger.error({ msg: "Import still in progress skipping chain", data: { chain: chainProducts$.key } });
        return Rx.EMPTY;
      }
      importStates[chainProducts$.key].inProgress = true;
      importStates[chainProducts$.key].start = new Date();
      const processor = processorByChain[chainProducts$.key];

      const pipeline$ = chainProducts$.pipe(
        Rx.toArray(),
        Rx.mergeMap((products) => processor(Rx.from(products))),

        // catch errors to avoid crashing when one chain fails
        Rx.catchError((err) => {
          logger.error({ msg: "Error processing chain", data: { chain: chainProducts$.key, err } });
          logger.error(err);
          return Rx.EMPTY;
        }),

        Rx.finalize(() => {
          const stop = new Date();
          const start = importStates[chainProducts$.key].start;
          importStates[chainProducts$.key].inProgress = false;
          logger.info({
            msg: "done importing chain",
            data: {
              chain: chainProducts$.key,
              duration: ((stop.getTime() - start.getTime()) / 1000).toFixed(2) + "s",
            },
          });
        }),
      );
      return pipeline$;
    }),
    Rx.mergeAll(),

    Rx.finalize(() => {
      logger.info({ msg: "done importing all chains" });
    }),
  );
}
