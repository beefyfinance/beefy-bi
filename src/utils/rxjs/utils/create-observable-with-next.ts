import * as Rx from "rxjs";

// https://stackoverflow.com/a/72442680/2523414
export function createObservableWithNext<T>(): {
  observable: Rx.Observable<T>;
  next: (value: T) => void;
  complete: () => void;
} {
  const subject = new Rx.Subject<T>();
  const observable = subject.asObservable();

  const next = (value: T) => subject.next(value);
  const complete = () => {
    subject.complete();
  };

  return {
    observable,
    next,
    complete,
  };
}
