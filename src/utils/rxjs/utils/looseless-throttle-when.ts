import * as Rx from "rxjs";
import { rootLogger } from "../../logger";

const logger = rootLogger.child({ module: "rxjs-utils", component: "looseless-throttle-when" });

export function looselessThrottleWhen<TObj>(options: {
  checkIntervalMs: number; // then every X ms
  checkIntervalJitterMs: number; // add a bit of jitter to the check interval
  shouldSend: (queue: TObj[]) => number; // how many items to send
}) {
  let objQueue: TObj[] = [];
  let sourceObsFinalized = false;

  const releaser$ = new Rx.Observable<() => Rx.Observable<TObj[]>>((subscriber) => {
    const poller = setInterval(() => {
      // no work
      if (objQueue.length <= 0) {
        if (sourceObsFinalized) {
          subscriber.complete();
        }
        return;
      }

      const howManyToSend = Math.min(options.shouldSend(objQueue), objQueue.length);
      if (howManyToSend <= 0) {
        return;
      }

      const objsToSend = objQueue.splice(0, howManyToSend);
      subscriber.next(() => Rx.of(objsToSend));
    }, options.checkIntervalMs + Math.random() * options.checkIntervalJitterMs);

    return () => {
      logger.trace({ msg: "Releaser unsubscribed" });
      clearInterval(poller);
    };
  });

  return Rx.pipe(
    // queue up items as they come in
    Rx.map((obj: TObj) => {
      objQueue.push(obj);
      return () => Rx.EMPTY as Rx.Observable<TObj[]>;
    }),
    // register the finalization of the source obs
    Rx.finalize(() => {
      logger.trace({ msg: "Source obs finalized" });
      sourceObsFinalized = true;
    }),

    // replace them with our releaser
    Rx.mergeWith(releaser$),

    // ensure we sent all objects
    Rx.map((fn) => fn()),
    Rx.concatAll(),
    Rx.concatAll(),
  );
}
