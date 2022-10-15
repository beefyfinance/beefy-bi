import * as Rx from "rxjs";
import { sleep } from "../../async";
import { bufferUntilKeyChanged } from "./buffer-until-key-changed";
import { consumeObservable } from "./consume-observable";

describe("bufferUntilKeyChanged", () => {
  it("should do simple buffering properly", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 3, 4, 4, 4, 5, 5, 5, 5, 4, 4, 5, 5, 5, 5]).pipe(
      bufferUntilKeyChanged({
        getKey: (x) => `${x}`,
        maxBufferSize: 100,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1], [2], [3, 3], [4, 4, 4], [5, 5, 5, 5], [4, 4], [5, 5, 5, 5]]);
  });

  it("should buffer when only one group is present", async () => {
    const pipeline$ = Rx.from([1, 1, 1]).pipe(
      bufferUntilKeyChanged({
        getKey: (x) => `${x}`,
        maxBufferSize: 100,
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

  it("should allow for a max buffer size to be set", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2]).pipe(
      bufferUntilKeyChanged({
        getKey: (x) => `${x}`,
        maxBufferSize: 3,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      [1, 1, 1],
      [1, 1, 1],
      [1, 1],
      [2, 2, 2],
      [2, 2],
    ]);
  });

  it("should not emit an empty buffer", async () => {
    const pipeline$ = Rx.from([]).pipe(
      bufferUntilKeyChanged({
        getKey: (x) => `${x}`,
        maxBufferSize: 100,
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

  it("should emit when we have only one entry", async () => {
    const pipeline$ = Rx.from([10]).pipe(
      bufferUntilKeyChanged({
        getKey: (x) => `${x}`,
        maxBufferSize: 100,
        maxBufferTimeMs: 1000,
        pollFrequencyMs: 10,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[10]]);
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
        await sleep(120);
        subscriber.next(1);
        subscriber.next(1);
        await sleep(120);
        subscriber.next(1);
        subscriber.next(5);
        subscriber.complete();
      })();
    }).pipe(
      bufferUntilKeyChanged({
        getKey: (x) => `${x}`,
        maxBufferSize: 100,
        maxBufferTimeMs: 100,
        pollFrequencyMs: 5,
        pollJitterMs: 0,
        logInfos: { msg: "test" },
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1, 1, 1, 1, 1], [1, 1], [1], [5]]);
  });
});
