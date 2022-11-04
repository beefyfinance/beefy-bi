import * as Rx from "rxjs";
import { ProgrammerError } from "../../programmer-error";

export function createObservableWithNext<T>(): {
  observable: Rx.Observable<T>;
  next: (value: T) => void;
  complete: () => void;
} {
  let subscriber: null | Rx.Subscriber<T> = null;

  // queue actions while the subscriber is not ready
  const actionsQueue: Array<{ what: "next"; next: T } | { what: "complete" }> = [];

  const observable = new Rx.Observable<T>((s) => {
    subscriber = s;

    for (const action of actionsQueue) {
      if (action.what === "next") {
        s.next(action.next);
      } else if (action.what === "complete") {
        s.complete();
      } else {
        // @ts-ignore (never)
        throw new ProgrammerError(`Unknown action ${action.what}`);
      }
    }

    return () => {
      subscriber = null;
    };
  });

  const next = (value: T) => {
    console.dir({ msg: "error emited", value });
    if (!subscriber) {
      actionsQueue.push({ what: "next", next: value });
    } else {
      subscriber.next(value);
    }
  };
  const complete = () => {
    if (!subscriber) {
      actionsQueue.push({ what: "complete" });
    } else {
      subscriber.complete();
      subscriber = null;
    }
  };

  return {
    observable,
    next,
    complete,
  };
}
