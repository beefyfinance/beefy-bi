import * as Rx from "rxjs";
import { consumeObservable } from "./consume-observable";
import { throttleWhen } from "./throttle-when";

describe("throttleWhen", () => {
  it("should not throttle when a passthrough is passed", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      throttleWhen({
        checkIntervalJitterMs: 0,
        checkIntervalMs: 0,
        sendBurstsOf: 1,
        shouldSend: () => true,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]);
  });

  it("should not fail when shouldSend returns a value larger than buffer size", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      throttleWhen({
        checkIntervalJitterMs: 0,
        checkIntervalMs: 50,
        sendBurstsOf: 100,
        shouldSend: () => true,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]);
  });

  // I have no idea how to test throttling and can't make marble diagrams work
});
