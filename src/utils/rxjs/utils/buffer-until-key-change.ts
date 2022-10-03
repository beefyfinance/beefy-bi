import * as Rx from "rxjs";

export function bufferUntilKeyChanged<TObj>(getKey: (obj: TObj) => string, maxBufferSize?: number): Rx.OperatorFunction<TObj, TObj[]> {
  let previousKey: string | null = null;
  let objBuffer = [] as TObj[];

  function flushBuffer(newBuffer: TObj[], newKey: string) {
    const buffer = objBuffer;
    objBuffer = newBuffer;
    previousKey = newKey;
    if (buffer.length === 0) {
      return Rx.EMPTY;
    } else {
      return Rx.of(() => buffer);
    }
  }

  return Rx.pipe(
    Rx.mergeMap((obj) => {
      const key = getKey(obj);

      if (maxBufferSize !== undefined && objBuffer.length >= maxBufferSize) {
        return flushBuffer([obj], key);
      }

      if (previousKey === null) {
        previousKey = key;
      }

      if (key === previousKey) {
        objBuffer.push(obj);
        return Rx.EMPTY;
      } else {
        return flushBuffer([obj], key);
      }
    }),

    Rx.endWith(() => objBuffer),
    Rx.filter((fn) => fn().length > 0),

    Rx.map((fn) => fn()),
  );
}
