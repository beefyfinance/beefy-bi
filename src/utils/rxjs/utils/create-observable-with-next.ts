import * as Rx from "rxjs";

export function createObservableWithNext<T>(): {
  observable: Rx.Observable<T>;
  next: (value: T) => void;
} {
  const subject = new Rx.Subject<T>();
  const observable = subject.asObservable();

  const next = (value: T) => subject.next(value);

  return {
    observable,
    next,
  };
}
