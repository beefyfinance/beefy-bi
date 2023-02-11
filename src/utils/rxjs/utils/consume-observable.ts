import { Observable, Subscription } from "rxjs";
import { rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "rxjs-utils", component: "consume-observable" });

export function consumeObservable<TRes>(observable: Observable<TRes>) {
  return new Promise<TRes | null>((resolve, reject) => {
    let lastValue: TRes | null = null;
    observable.subscribe({
      error: reject,
      next: (value) => (lastValue = value),
      complete: () => resolve(lastValue),
    });
  });
}

export function consumeStoppableObservable<TRes>(observable: Observable<TRes>) {
  let subscription: Subscription | null = null;
  let lastValue: TRes | null = null;
  let hasResolved = false;
  let promiseResolve: ((value: TRes | PromiseLike<TRes | null> | null) => void) | null = null;

  function doResolve() {
    if (!hasResolved && promiseResolve !== null) {
      hasResolved = true;
      promiseResolve(lastValue);
    }
  }

  const promise = new Promise<TRes | null>((resolve, reject) => {
    promiseResolve = resolve;
    subscription = observable.subscribe({
      error: reject,
      next: (value) => (lastValue = value),
      complete: doResolve,
    });
  });

  function stop() {
    if (subscription) {
      try {
        if (!subscription.closed) {
          subscription.unsubscribe();
          doResolve();
        }
      } catch (e) {
        logger.error({ msg: "Error while unsubscribing", data: e });
        logger.trace(e);
      }
    }
  }
  return {
    promise,
    stop,
  };
}
