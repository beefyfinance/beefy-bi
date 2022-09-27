import * as Rx from "rxjs";
import { bufferUntilKeyChanged } from "./buffer-until-key-change";
import { consumeObservable } from "./consume-observable";

describe("bufferUntilKeyChanged", () => {
  it("should do simple buffering properly", async () => {
    const pipeline$ = Rx.from([1, 1, 1, 2, 3, 3, 4, 4, 4, 5, 5, 5, 5, 4, 4, 5, 5, 5, 5]).pipe(
      bufferUntilKeyChanged((x) => `${x}`),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([[1, 1, 1], [2], [3, 3], [4, 4, 4], [5, 5, 5, 5], [4, 4], [5, 5, 5, 5]]);
  });
});
