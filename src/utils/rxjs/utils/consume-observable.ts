import { Observable } from "rxjs";

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
