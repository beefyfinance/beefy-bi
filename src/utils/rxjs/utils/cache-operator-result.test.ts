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
        stdTTLSec: 10,
        formatOutput: (x, x2) => `${x}^2 = ${x2}`,
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual(["1^2 = 2", "1^2 = 2", "1^2 = 2", "2^2 = 4", "3^2 = 6", "2^2 = 4", "1^2 = 2", "2^2 = 4", "1^2 = 2", "3^2 = 6"]);
    expect(callCount).toEqual(3);
  });
});
