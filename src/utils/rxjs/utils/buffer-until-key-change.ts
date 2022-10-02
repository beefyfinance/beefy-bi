import * as Rx from "rxjs";

export function bufferUntilKeyChanged<TObj>(getKey: (obj: TObj) => string): Rx.OperatorFunction<TObj, TObj[]> {
  let previousKey: string | null = null;
  let objBuffer = [] as TObj[];

  return Rx.pipe(
    Rx.mergeMap((obj) => {
      const key = getKey(obj);
      if (previousKey === null) {
        previousKey = key;
      }

      if (key === previousKey) {
        objBuffer.push(obj);
        return Rx.EMPTY;
      } else {
        const buffer = objBuffer;
        objBuffer = [obj];
        previousKey = key;
        return Rx.of(() => buffer);
      }
    }),

    Rx.endWith(() => objBuffer),

    Rx.map((fn) => fn()),
  );
}
