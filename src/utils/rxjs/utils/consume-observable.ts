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
  const promise = new Promise<TRes | null>((resolve, reject) => {
    let lastValue: TRes | null = null;
    subscription = observable.subscribe({
      error: reject,
      next: (value) => (lastValue = value),
      complete: () => resolve(lastValue),
    });
  });
  function stop() {
    if (subscription) {
      try {
        if (!subscription.closed) {
          subscription.unsubscribe();
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
