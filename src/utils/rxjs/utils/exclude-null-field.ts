import * as Rx from "rxjs";
import { rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "common", component: "exclude-null-field" });

// https://stackoverflow.com/a/73083447/2523414
type NonNullFields<T> = {
  [P in keyof T]: NonNullable<T[P]>;
};
export type NonNullField<T, K extends keyof T> = Omit<Omit<T, K> & NonNullFields<Pick<T, K>>, "">;

type NullFields<T> = {
  [P in keyof T]: null;
};
export type NullField<T, K extends keyof T> = Omit<Omit<T, K> & NullFields<Pick<T, K>>, "">;

export function excludeNullFields$<TObj, TKey extends keyof TObj, TRes extends NonNullField<TObj, TKey>>(
  key: TKey,
): Rx.OperatorFunction<TObj, NonNullField<TObj, TKey>> {
  return Rx.pipe(
    // @ts-ignore
    Rx.filter((obj): obj is TRes => {
      const keep = obj[key] !== null;
      if (!keep) {
        logger.trace({ msg: "excluding obj with null field", data: { key, obj } });
      }
      return keep;
    }),
    Rx.map((obj): TRes => obj as unknown as TRes),
  );
}
export function excludeNotNullFields$<TObj, TKey extends keyof TObj, TRes extends NullField<TObj, TKey>>(
  key: TKey,
): Rx.OperatorFunction<TObj, NullField<TObj, TKey>> {
  return Rx.pipe(
    // @ts-ignore
    Rx.filter((obj): obj is TRes => {
      const keep = obj[key] === null;
      if (!keep) {
        logger.trace({ msg: "excluding obj with non-null field", data: { key, obj } });
      }
      return keep;
    }),
    Rx.map((obj): TRes => obj as unknown as TRes),
  );
}

export function forkOnNullableField$<TObj, TKey extends keyof TObj, TRes>({
  key,
  handleNulls$ = Rx.pipe() as Rx.OperatorFunction<NullField<TObj, TKey>, TRes>,
  handleNonNulls$ = Rx.pipe() as Rx.OperatorFunction<NonNullField<TObj, TKey>, TRes>,
}: {
  key: TKey;
  handleNulls$?: Rx.OperatorFunction<NullField<TObj, TKey>, TRes>;
  handleNonNulls$?: Rx.OperatorFunction<NonNullField<TObj, TKey>, TRes>;
}): Rx.OperatorFunction<TObj, TRes> {
  return Rx.pipe(
    Rx.connect((items$) => Rx.merge(items$.pipe(excludeNullFields$(key), handleNonNulls$), items$.pipe(excludeNotNullFields$(key), handleNulls$))),
  );
}
