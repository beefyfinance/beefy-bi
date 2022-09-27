import * as Rx from "rxjs";

export function bufferUntilKeyChanged<TObj>(getKey: (obj: TObj) => string): Rx.OperatorFunction<TObj, TObj[]> {
  // this is very inefficient, but it works
  return Rx.pipe(
    Rx.map((obj) => ({ key: getKey(obj), obj })),

    Rx.pairwise(),

    Rx.map(([prev, curr]) => ({ prev, curr })),

    Rx.scan(
      (acc, { prev, curr }) => {
        if (prev.key === curr.key) {
          if (acc.objs.length === 0) {
            return { complete: false, objs: [prev.obj, curr.obj] };
          } else {
            return { complete: false, objs: [...acc.objs, curr.obj] };
          }
        } else {
          return { complete: true, objs: [curr.obj] };
        }
      },
      { complete: false, objs: [] as TObj[] },
    ),

    Rx.mergeWith(Rx.of({ complete: true, objs: [] as TObj[] })),

    Rx.pairwise(),

    Rx.mergeMap(([prev, curr]) => {
      if (curr.complete) {
        return Rx.of(prev.objs);
      } else {
        return Rx.EMPTY;
      }
    }),
  );
}
