import * as Rx from "rxjs";
import { RpcConfig } from "../../../types/rpc-config";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { _getRpcWeight, _weightedDistribute } from "./rpc-chain-runner";

describe("createChainRunner$", () => {
  it("should compute weight according to the limitations", () => {
    const getConfig = (delay: number | "no-limit"): RpcConfig =>
      ({
        rpcLimitations: {
          minDelayBetweenCalls: delay,
        },
      } as any);

    expect(_getRpcWeight(getConfig("no-limit"))).toEqual(10_000);
    expect(_getRpcWeight(getConfig(1000))).toEqual(1_000);
    expect(_getRpcWeight(getConfig(100))).toEqual(2_000);
    expect(_getRpcWeight(getConfig(10))).toEqual(2_000);
  });

  it("should distribute according to weight", async () => {
    const input = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }];
    const randomMock = jest.fn();
    randomMock.mockReturnValueOnce(1).mockReturnValueOnce(2).mockReturnValueOnce(1).mockReturnValueOnce(11);
    const weights = [{ weight: 1 }, { weight: 10 }];
    const res = _weightedDistribute(input, weights, randomMock);
    expect(res).toEqual(
      new Map([
        [weights[0], [input[0], input[2]]],
        [weights[1], [input[1], input[3]]],
      ]),
    );
  });
});
