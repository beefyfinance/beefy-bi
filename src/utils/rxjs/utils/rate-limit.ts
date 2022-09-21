import * as Rx from "rxjs";

// adapted from https://stackoverflow.com/a/64863864/2523414
export function rateLimit$<TObj>(delayMs: number): Rx.OperatorFunction<TObj, TObj> {
  return Rx.pipe(
    Rx.concatMap((obj) =>
      Rx.timer(delayMs).pipe(
        Rx.filter((_) => false),
        Rx.startWith(obj),
        Rx.map((_) => obj),
      ),
    ),
  );
}
