import * as Rx from "rxjs";
import { RpcConfig } from "../../../types/rpc-config";
import { ProgrammerError } from "../../../utils/programmer-error";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { getWeight, weightedMultiplex } from "./multiplex-by-rpc";

describe("multiplexByRcp$", () => {
  it("should pass all items when only one RPC is available", async () => {
    const input$ = Rx.from([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);

    const pipeline$ = input$.pipe(weightedMultiplex([{ pipeline: Rx.pipe(Rx.map((obj) => ({ obj, pipe: 0 }))), weight: 1 }]), Rx.toArray());

    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      { obj: { id: 1 }, pipe: 0 },
      { obj: { id: 2 }, pipe: 0 },
      { obj: { id: 3 }, pipe: 0 },
      { obj: { id: 4 }, pipe: 0 },
    ]);
  });

  it("should throw if some weight is invalid (<=0)", async () => {
    await expect(async () => weightedMultiplex([{ pipeline: Rx.pipe(), weight: 0 }])).rejects.toThrow(ProgrammerError);
    await expect(async () => weightedMultiplex([{ pipeline: Rx.pipe(), weight: -1 }])).rejects.toThrow(ProgrammerError);
  });

  it("should distribute according to rng generator", async () => {
    const input$ = Rx.from([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);

    const randomMock = jest.fn();
    randomMock.mockReturnValueOnce(1).mockReturnValueOnce(2).mockReturnValueOnce(1).mockReturnValueOnce(2);
    const pipeline$ = input$.pipe(
      weightedMultiplex(
        [
          { pipeline: Rx.pipe(Rx.map((obj) => ({ obj, pipe: 1 }))), weight: 1 },
          { pipeline: Rx.pipe(Rx.map((obj) => ({ obj, pipe: 2 }))), weight: 1 },
        ],
        randomMock,
      ),
      Rx.toArray(),
    );

    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      { obj: { id: 1 }, pipe: 1 },
      { obj: { id: 2 }, pipe: 2 },
      { obj: { id: 3 }, pipe: 1 },
      { obj: { id: 4 }, pipe: 2 },
    ]);
    expect(randomMock.mock.calls).toEqual([
      [1, 2, false],
      [1, 2, false],
      [1, 2, false],
      [1, 2, false],
    ]);
  });

  it("should distribute according to weight", async () => {
    const input$ = Rx.from([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);

    const randomMock = jest.fn();
    randomMock.mockReturnValueOnce(1).mockReturnValueOnce(2).mockReturnValueOnce(1).mockReturnValueOnce(11);
    const pipeline$ = input$.pipe(
      weightedMultiplex(
        [
          { pipeline: Rx.pipe(Rx.map((obj) => ({ obj, pipe: 1 }))), weight: 1 },
          { pipeline: Rx.pipe(Rx.map((obj) => ({ obj, pipe: 2 }))), weight: 10 },
        ],
        randomMock,
      ),
      Rx.toArray(),
    );

    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      { obj: { id: 1 }, pipe: 1 },
      { obj: { id: 2 }, pipe: 2 },
      { obj: { id: 3 }, pipe: 1 },
      { obj: { id: 4 }, pipe: 2 },
    ]);
    expect(randomMock.mock.calls).toEqual([
      [1, 11, false],
      [1, 11, false],
      [1, 11, false],
      [1, 11, false],
    ]);
  });

  it("should compute weight according to the limitations", () => {
    const getConfig = (delay: number | "no-limit"): RpcConfig =>
      ({
        rpcLimitations: {
          minDelayBetweenCalls: delay,
        },
      } as any);

    expect(getWeight(getConfig("no-limit"))).toEqual(10_000);
    expect(getWeight(getConfig(1000))).toEqual(1_000);
    expect(getWeight(getConfig(100))).toEqual(2_000);
    expect(getWeight(getConfig(10))).toEqual(2_000);
  });
});
