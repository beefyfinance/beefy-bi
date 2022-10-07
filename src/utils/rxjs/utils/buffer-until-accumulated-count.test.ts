import * as Rx from "rxjs";
import { sleep } from "../../async";
import { bufferUntilAccumulatedCountReached } from "./buffer-until-accumulated-count";
import { consumeObservable } from "./consume-observable";

describe("bufferUntilAccumulatedCountReachedOrTimePassed", () => {
  it("should do simple buffering properly", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      bufferUntilAccumulatedCountReached({
        getCount: (x) => x,
        maxBufferSize: 5,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1, 2], [3, 2], [1, 2, 1], [3]]);
  });

  it("should buffer when only one group is present", async () => {
    const pipeline$ = Rx.from([1, 1, 1]).pipe(
      bufferUntilAccumulatedCountReached({
        getCount: (x) => x,
        maxBufferSize: 5,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1]]);
  });

  it("should send one if item is already too big present", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 10, 1, 1, 1]).pipe(
      bufferUntilAccumulatedCountReached({
        getCount: (x) => x,
        maxBufferSize: 5,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1], [10], [1, 1, 1]]);
  });

  it("should not emit an empty buffer", async () => {
    const pipeline$ = Rx.from([]).pipe(
      bufferUntilAccumulatedCountReached({
        getCount: (x) => x,
        maxBufferSize: 5,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([]);
  });

  it("should send buffer if enough time has passed", async () => {
    const pipeline$ = new Rx.Observable<number>((subscriber) => {
      (async () => {
        subscriber.next(1);
        subscriber.next(1);
        subscriber.next(1);
        await sleep(10);
        subscriber.next(1);
        subscriber.next(1);
        await sleep(10);
        subscriber.next(1);
        subscriber.next(1);
        await sleep(100);
        subscriber.next(1);
        subscriber.next(1);
        await sleep(100);
        subscriber.next(1);
        subscriber.next(5);
        subscriber.complete();
      })();
    }).pipe(
      bufferUntilAccumulatedCountReached({
        getCount: (x) => x,
        maxBufferSize: 5,
        maxBufferTimeMs: 100,
        pollFrequencyMs: 1,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1, 1, 1], [1, 1], [1, 1], [1], [5]]);
  });
});
