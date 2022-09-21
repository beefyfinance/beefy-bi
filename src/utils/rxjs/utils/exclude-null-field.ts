import * as Rx from "rxjs";

// https://stackoverflow.com/a/73083447/2523414
type NonNullableFields<T> = {
  [P in keyof T]: NonNullable<T[P]>;
};
type NonNullableField<T, K extends keyof T> = T & NonNullableFields<Pick<T, K>>;

export function excludeNullFields$<TObj, TKey extends keyof TObj>(
  key: TKey,
): Rx.OperatorFunction<TObj, NonNullableField<TObj, TKey>> {
  return Rx.pipe(Rx.filter((obj): obj is NonNullableField<TObj, TKey> => obj[key] !== null));
}
