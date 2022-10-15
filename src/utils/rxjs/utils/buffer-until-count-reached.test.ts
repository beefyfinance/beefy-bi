import * as Rx from "rxjs";
import { sleep } from "../../async";
import { bufferUntilCountReached } from "./buffer-until-count-reached";
import { consumeObservable } from "./consume-observable";

describe("bufferUntilCountReached", () => {
  it("should do simple buffering properly", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      bufferUntilCountReached({
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
      bufferUntilCountReached({
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
      bufferUntilCountReached({
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
      bufferUntilCountReached({
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
        await sleep(120); //
        subscriber.next(1);
        subscriber.next(1);
        await sleep(120);
        subscriber.next(1);
        subscriber.next(5);
        subscriber.complete();
      })();
    }).pipe(
      bufferUntilCountReached({
        getCount: (x) => x,
        maxBufferSize: 5,
        maxBufferTimeMs: 100,
        pollFrequencyMs: 5,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1, 1, 1], [1, 1], [1, 1], [1], [5]]);
  });
});
