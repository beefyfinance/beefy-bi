import * as Rx from "rxjs";
import { ProgrammerError } from "../../../utils/programmer-error";
import { consumeObservable } from "../../../utils/rxjs/utils/consume-observable";
import { executeSubPipeline$ } from "./execute-sub-pipeline";

describe("executeSubPipeline$", () => {
  it("should execute a sub pipeline properly", async () => {
    const input$ = Rx.from([
      { id: 1, targets: [1, 2, 3] },
      { id: 2, targets: [4, 5, 6] },
      { id: 3, targets: [7, 8, 9] },
      { id: 4, targets: [] },
    ]);

    const errors: any[] = [];
    const ctx: any = { streamConfig: { maxBatchSize: 100, maxInputWaitMs: 100 } };

    const pipeline$ = input$.pipe(
      executeSubPipeline$({
        ctx,
        emitError: (obj) => errors.push(obj),
        getObjs: async (obj) => obj.targets,
        pipeline: (emitError) => Rx.pipe(Rx.map((item) => ({ ...item, result: item.target * 2 }))),
        formatOutput: (obj, result) => ({ ...obj, result: obj.targets.map((t) => result.get(t)) }),
      }),
      Rx.toArray(),
    );
    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      { id: 1, targets: [1, 2, 3], result: [2, 4, 6] },
      { id: 2, targets: [4, 5, 6], result: [8, 10, 12] },
      { id: 3, targets: [7, 8, 9], result: [14, 16, 18] },
      { id: 4, targets: [], result: [] },
    ]);
  });

  it("should fail if sub pipeline skipped some objects", async () => {
    const input$ = Rx.from([
      { id: 1, targets: [1, 2, 3] },
      { id: 2, targets: [4, 5, 6] },
      { id: 3, targets: [7, 8, 9] },
    ]);

    const errors: any[] = [];
    const ctx: any = { streamConfig: { maxBatchSize: 100, maxInputWaitMs: 100 } };

    const pipeline$ = input$.pipe(
      executeSubPipeline$({
        ctx,
        emitError: (obj) => errors.push(obj),
        getObjs: async (obj) => obj.targets,
        pipeline: (emitError) =>
          Rx.pipe(
            Rx.filter((item) => item.target !== 5),
            Rx.map((item) => ({ ...item, result: item.target * 2 })),
          ),
        formatOutput: (obj, result) => ({ ...obj, result: obj.targets.map((t) => result.get(t)) }),
      }),
      Rx.toArray(),
    );
    await expect(() => consumeObservable(pipeline$)).rejects.toThrow(ProgrammerError);
  });

  it("should propagate sub items errors to item errors", async () => {
    const input$ = Rx.from([
      { id: 1, targets: [1, 2, 3] },
      { id: 2, targets: [4, 5, 6] },
      { id: 3, targets: [7, 8, 9] },
    ]);

    const errors: any[] = [];
    const ctx: any = { streamConfig: { maxBatchSize: 100, maxInputWaitMs: 100 } };

    const pipeline$ = input$.pipe(
      executeSubPipeline$({
        ctx,
        emitError: (obj) => errors.push(obj),
        getObjs: async (obj) => obj.targets,
        pipeline: (emitError) =>
          Rx.pipe(
            Rx.filter((item) => {
              if (item.target === 5) {
                emitError(item, { infos: { msg: "error" } });
                return false;
              }
              return true;
            }),
            Rx.map((item) => ({ ...item, result: item.target * 2 })),
          ),
        formatOutput: (obj, result) => ({ ...obj, result: obj.targets.map((t) => result.get(t)) }),
      }),
      Rx.toArray(),
    );

    const result = await consumeObservable(pipeline$);
    expect(result).toEqual([
      { id: 1, targets: [1, 2, 3], result: [2, 4, 6] },
      { id: 3, targets: [7, 8, 9], result: [14, 16, 18] },
    ]);
    expect(errors).toEqual([{ id: 2, targets: [4, 5, 6] }]);
  });

  it("should detect when sub-pipeline returns both failed and success for an item", async () => {
    const input$ = Rx.from([
      { id: 1, targets: [1, 2, 3] },
      { id: 2, targets: [4, 5, 6] },
      { id: 3, targets: [7, 8, 9] },
    ]);

    const errors: any[] = [];
    const ctx: any = { streamConfig: { maxBatchSize: 100, maxInputWaitMs: 100 } };

    const pipeline$ = input$.pipe(
      executeSubPipeline$({
        ctx,
        emitError: (obj) => errors.push(obj),
        getObjs: async (obj) => obj.targets,
        pipeline: (emitError) =>
          Rx.pipe(
            Rx.filter((item) => {
              if (item.target === 5) {
                emitError(item, { infos: { msg: "error" } });
              }
              return true;
            }),
            Rx.map((item) => ({ ...item, result: item.target * 2 })),
          ),
        formatOutput: (obj, result) => ({ ...obj, result: obj.targets.map((t) => result.get(t)) }),
      }),
      Rx.toArray(),
    );

    await expect(() => consumeObservable(pipeline$)).rejects.toThrow(ProgrammerError);
  });

  it("should detect when sub-pipeline returns a sub-item multiple times", async () => {
    const input$ = Rx.from([
      { id: 1, targets: [1, 2, 3] },
      { id: 2, targets: [4, 5, 6] },
      { id: 3, targets: [7, 8, 9] },
    ]);

    const errors: any[] = [];
    const ctx: any = { streamConfig: { maxBatchSize: 100, maxInputWaitMs: 100 } };

    const pipeline$ = input$.pipe(
      executeSubPipeline$({
        ctx,
        emitError: (obj) => errors.push(obj),
        getObjs: async (obj) => obj.targets,
        pipeline: (emitError) =>
          Rx.pipe(
            Rx.connect((items$) =>
              Rx.merge(
                items$.pipe(
                  Rx.filter((item) => item.parent.id !== 2),
                  // do nothing
                ),
                items$.pipe(
                  Rx.filter((item) => item.parent.id === 2),
                  // duplicate sub-items
                  Rx.concatMap((item) => [item, item, item]),
                ),
              ),
            ),
            Rx.map((item) => ({ ...item, result: item.target * 2 })),
          ),
        formatOutput: (obj, result) => ({ ...obj, result: obj.targets.map((t) => result.get(t)) }),
      }),
      Rx.toArray(),
    );

    await expect(() => consumeObservable(pipeline$)).rejects.toThrow(ProgrammerError);
  });
});
