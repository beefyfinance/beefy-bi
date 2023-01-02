import NodeCache from "node-cache";
import * as Rx from "rxjs";
import { LogInfos, mergeLogsInfos, rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "rxjs-utils", component: "cache-operator-result" });

export function cacheOperatorResult$<TObj, TRes, TOutput>(options: {
  stdTTLSec: number;
  getCacheKey: (input: TObj) => string;
  logInfos: LogInfos;
  operator$: Rx.OperatorFunction<TObj, { input: TObj; output: TOutput }>;
  formatOutput: (input: TObj, output: TOutput) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const cache = new NodeCache({ stdTTL: options.stdTTLSec });
  return Rx.pipe(
    Rx.map((item) => ({ obj: item, cacheKey: options.getCacheKey(item) })),
    Rx.connect((item$) =>
      Rx.merge(
        item$.pipe(
          Rx.filter((item) => cache.has(item.cacheKey)),
          Rx.tap((item) => logger.trace(mergeLogsInfos({ msg: "cache hit", data: { cacheKey: item.cacheKey } }, options.logInfos))),
          Rx.map((item) => ({ ...item, result: cache.get(item.cacheKey) as TOutput })),
          Rx.map(({ obj, result }) => options.formatOutput(obj, result)),
        ),
        item$.pipe(
          Rx.filter((item) => !cache.has(item.cacheKey)),
          Rx.tap((item) => logger.trace(mergeLogsInfos({ msg: "cache miss", data: { cacheKey: item.cacheKey } }, options.logInfos))),
          Rx.map(({ obj }) => obj),
          options.operator$,
          Rx.tap(({ input, output }) => {
            const cacheKey = options.getCacheKey(input);
            // find out if some other concurrent call already set the cache
            // so we avoid serializing the same result multiple times
            if (!cache.has(cacheKey)) {
              cache.set(cacheKey, output);
              logger.trace(mergeLogsInfos({ msg: "cache set", data: { cacheKey } }, options.logInfos));
            } else {
              logger.trace(mergeLogsInfos({ msg: "cache already set", data: { cacheKey } }, options.logInfos));
            }
          }),
          Rx.map(({ input, output }) => options.formatOutput(input, output)),
        ),
      ),
    ),
  );
}
