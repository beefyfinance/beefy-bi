import * as Rx from "rxjs";
import { allChainIds, Chain } from "../../../types/chain";
import { rootLogger } from "../../../utils/logger";

const logger = rootLogger.child({ module: "loader", component: "by-chain" });

export function loaderByChain$<TInput extends { chain: Chain }, TOutput>(
  processChain: (chain: Chain) => Rx.OperatorFunction<TInput, TOutput>,
): Rx.OperatorFunction<TInput, TOutput> {
  type ImportState = { inProgress: boolean; start: Date };
  const importStates: Record<Chain, ImportState> = allChainIds.reduce(
    (acc, chain) => Object.assign(acc, { [chain]: { inProgress: false, start: new Date() } }),
    {} as Record<Chain, ImportState>,
  );

  return (objs$) =>
    objs$.pipe(
      // create a different sub-pipeline for each chain
      Rx.groupBy((vault) => vault.chain),

      Rx.tap((chainVaults$) => logger.debug({ msg: "processing chain", data: { chain: chainVaults$.key } })),

      // process each chain separately
      Rx.mergeMap((chainVaults$) => {
        if (importStates[chainVaults$.key].inProgress) {
          logger.error({ msg: "Import still in progress skipping chain", data: { chain: chainVaults$.key } });
          return Rx.EMPTY;
        }
        importStates[chainVaults$.key].inProgress = true;
        importStates[chainVaults$.key].start = new Date();
        return processChain(chainVaults$.key)(chainVaults$).pipe(
          Rx.tap({
            finalize: () => {
              const stop = new Date();
              const start = importStates[chainVaults$.key].start;
              importStates[chainVaults$.key].inProgress = false;
              logger.info({
                msg: "done importing chain",
                data: {
                  chain: chainVaults$.key,
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
