import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "loader", component: "by-chain" });

export function loaderByChain$<TInput extends { chain: Chain; productId: number }, TOutput>(
  processChain: (chain: Chain) => Rx.OperatorFunction<TInput, TOutput>,
): Rx.OperatorFunction<TInput, TOutput> {
  type ImportState = { inProgress: boolean; start: Date };
  const importStates: Record<Chain, ImportState> = allChainIds.reduce(
    (acc, chain) => Object.assign(acc, { [chain]: { inProgress: false, start: new Date() } }),
    {} as Record<Chain, ImportState>,
  );
  let index = 0;

  return Rx.pipe(
    // create a different sub-pipeline for each chain
    Rx.groupBy((product) => product.chain),

    Rx.tap((chainProducts$) => logger.debug({ msg: "processing chain", data: { chain: chainProducts$.key } })),

    // process each chain separately
    Rx.concatMap((chainProducts$) => {
      console.log(index++, "chain", chainProducts$.key);
      if (importStates[chainProducts$.key].inProgress) {
        logger.error({ msg: "Import still in progress skipping chain", data: { chain: chainProducts$.key } });
        return Rx.EMPTY;
      }
      importStates[chainProducts$.key].inProgress = true;
      importStates[chainProducts$.key].start = new Date();
      const productProcess = processChain(chainProducts$.key);
      return chainProducts$.pipe(
        // I have no idea why this is needed but it is
        // somehow the group by is duplicating entries
        Rx.distinctUntilKeyChanged("productId"),

        Rx.tap((product) => console.log("products sent for processing", product.productId, chainProducts$.key)),

        productProcess,

        Rx.tap({
          finalize: () => {
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
          },
        }),
      );
    }),

    Rx.tap({
      finalize: () => {
        logger.info({ msg: "done importing all chains" });
      },
    }),
  );
}
