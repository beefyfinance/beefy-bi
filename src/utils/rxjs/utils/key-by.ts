import * as Rx from "rxjs";

export function keyBy$<TObj>(getKey: (obj: TObj) => string): Rx.OperatorFunction<TObj, Record<string, TObj>> {
  return Rx.pipe(
    Rx.reduce((acc, obj) => {
      acc[getKey(obj)] = obj;
      return acc;
    }, {} as Record<string, TObj>),
  );
}
