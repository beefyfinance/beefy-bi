import * as Rx from "rxjs";
import { rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "common", component: "investment" });

// https://stackoverflow.com/a/73083447/2523414
type NonNullableFields<T> = {
  [P in keyof T]: NonNullable<T[P]>;
};
type NonNullableField<T, K extends keyof T> = T & NonNullableFields<Pick<T, K>>;

export function excludeNullFields$<TObj, TKey extends keyof TObj>(key: TKey): Rx.OperatorFunction<TObj, NonNullableField<TObj, TKey>> {
  return Rx.pipe(
    Rx.filter((obj): obj is NonNullableField<TObj, TKey> => {
      const keep = obj[key] !== null;
      if (!keep) {
        logger.trace({ msg: "excluding obj with null field", data: { key, obj } });
      }
      return keep;
    }),
  );
}
