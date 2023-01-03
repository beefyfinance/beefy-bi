import * as Rx from "rxjs";
import { cacheOperatorResult$ } from "./cache-operator-result";
import { consumeObservable } from "./consume-observable";

describe("cacheOperatorResult", () => {
  it("should call the operator the first time", async () => {
    let callCount = 0;

    const operator$ = Rx.pipe(
      Rx.map((input: number) => {
        callCount++;
        return { input, output: input * 2 };
      }),
    );

    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          stdTTLSec: 10,
          type: "local",
          useClones: false,
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(3);
  });

  it("should have a local cache option", async () => {
    let callCount = 0;

    const operator$ = Rx.pipe(
      Rx.map((input: number) => {
        callCount++;
        return { input, output: input * 2 };
      }),
    );

    const pipeline1$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          stdTTLSec: 10,
          type: "local",
          useClones: false,
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result1 = await consumeObservable(pipeline1$);
    expect(result1).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(3);

    const pipeline2$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          stdTTLSec: 10,
          type: "local",
          useClones: false,
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result2 = await consumeObservable(pipeline2$);
    expect(result2).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(6);
  });

  it("should have a global cache option", async () => {
    let callCount = 0;

    const operator$ = Rx.pipe(
      Rx.map((input: number) => {
        callCount++;
        return { input, output: input * 2 };
      }),
    );

    const pipeline1$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          type: "global",
          stdTTLSec: 10,
          useClones: false,
          globalKey: "test",
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result1 = await consumeObservable(pipeline1$);
    expect(result1).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(3);

    const pipeline2$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          type: "global",
          stdTTLSec: 10,
          useClones: false,
          globalKey: "test",
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result2 = await consumeObservable(pipeline2$);
    expect(result2).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(3);
  });

  it("should have multiple global caches option", async () => {
    let callCount = 0;

    const operator$ = Rx.pipe(
      Rx.map((input: number) => {
        callCount++;
        return { input, output: input * 2 };
      }),
    );

    const pipeline1$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          type: "global",
          stdTTLSec: 10,
          useClones: false,
          globalKey: "test_1",
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result1 = await consumeObservable(pipeline1$);
    expect(result1).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(3);

    const pipeline2$ = Rx.from([1, 1, 1, 2, 3, 2, 1, 2, 1, 3]).pipe(
      cacheOperatorResult$({
        getCacheKey: (n) => `${n}`,
        operator$: operator$,
        logInfos: { msg: "test" },
        cacheConfig: {
          type: "global",
          stdTTLSec: 10,
          useClones: false,
          globalKey: "test_2",
        },
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result2 = await consumeObservable(pipeline2$);
    expect(result2).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(6);
  });
});
