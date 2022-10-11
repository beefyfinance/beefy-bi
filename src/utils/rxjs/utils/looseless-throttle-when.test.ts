import * as Rx from "rxjs";
import { looselessThrottleWhen } from "./looseless-throttle-when";
import { consumeObservable } from "./consume-observable";

describe("looselessThrottleWhen", () => {
  it("should not throttle when a passthrough is passed", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      looselessThrottleWhen({
        checkIntervalJitterMs: 0,
        checkIntervalMs: 0,
        shouldSend: () => 1,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]);
  });

  it("should not fail when shouldSend returns a value larger than buffer size", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      looselessThrottleWhen({
        checkIntervalJitterMs: 0,
        checkIntervalMs: 50,
        shouldSend: () => 100,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]);
  });

  // I have no idea how to test throttling and can't make marble diagrams work
});
