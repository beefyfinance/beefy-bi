import { get, sum } from "lodash";
import * as Rx from "rxjs";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { createObservableWithNext } from "../../../utils/rxjs/utils/create-observable-with-next";
import { ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "execute-sub-pipeline" });

/**
 * An rjxs operator that executes a sub pipeline.
 * Each value can yield an array that get passed to the sub pipeline.
 * If any error is yielded from any value of the sub pipeline, the current value is consided as failed.
 * Do cross value batching as much as possible
 */
export function executeSubPipeline$<TObj, TCtx extends ImportCtx<TObj>, TRes, TSubPipelineInput, TSubPipelineRes>(options: {
  ctx: TCtx;
  getObjs: (item: TObj) => TSubPipelineInput[];
  pipeline: (
    ctx: ImportCtx<{ target: TSubPipelineInput; parent: TObj }>,
  ) => Rx.OperatorFunction<{ target: TSubPipelineInput; parent: TObj }, { target: TSubPipelineInput; parent: TObj; result: TSubPipelineRes }>;
  formatOutput: (obj: TObj, subPipelineResult: Map<TSubPipelineInput, TSubPipelineRes>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  // @ts-ignore
  return Rx.pipe(
    // work by batches of 300 objs
    Rx.pipe(
      Rx.bufferTime(options.ctx.streamConfig.maxInputWaitMs, undefined, 300),
      Rx.filter((objs) => objs.length > 0),

      Rx.map((objs) => objs.map((obj) => ({ obj, targets: options.getObjs(obj) }))),
      Rx.tap((items) =>
        logger.debug({
          msg: "Importing a batch of values",
          data: { objCount: items.length, targetCount: sum(items.map((i) => i.targets.length)) },
        }),
      ),
    ),

    Rx.mergeMap((items) => {
      const {
        observable: pipelineErrors$,
        next: emitPipelineErrors,
        complete: completePipelineErrors$,
      } = createObservableWithNext<{ target: TSubPipelineInput; parent: TObj }>();

      const pipelineCtx = {
        ...options.ctx,
        emitErrors: emitPipelineErrors,
      };

      const inputs = items.flatMap((item) => item.targets.map((target) => ({ target, parent: item.obj })));
      return Rx.of(inputs).pipe(
        Rx.mergeAll(),

        Rx.tap((item) => logger.trace({ msg: "importing sub-item", data: item })),

        // execute the sub pipeline
        options.pipeline(pipelineCtx),

        // merge errors
        Rx.pipe(
          Rx.map((item) => ({ ...item, success: get(item, "success", true) as boolean })),
          // make sure we close the errors observable when we are done
          Rx.tap({ complete: () => setTimeout(completePipelineErrors$) }),
          // merge the errors back in, all items here should have been successfully treated
          Rx.mergeWith(pipelineErrors$.pipe(Rx.map((item) => ({ ...item, success: false })))),
          // make sure the type is correct
          Rx.map((item): { parent: TObj; target: TSubPipelineInput; result?: TSubPipelineRes; success: boolean } => item),
        ),

        // return to product representation
        Rx.toArray(),
        Rx.map((subItems) => {
          logger.trace({ msg: "imported sub-items", data: { itemCount: items.length, subItems: subItems.length } });
          // make sure every sub item is present
          // if not, we consider the whole item as failed
          // this is to make sure we don't miss any error
          type MapRes = { success: true; result: TSubPipelineRes } | { success: false };
          const targetsByParent = new Map<TObj, Map<TSubPipelineInput, MapRes>>();
          for (const subItem of subItems) {
            if (!targetsByParent.has(subItem.parent)) {
              targetsByParent.set(subItem.parent, new Map());
            }
            const res: MapRes = subItem.result ? { success: true, result: subItem.result } : { success: false };
            targetsByParent.get(subItem.parent)?.set(subItem.target, res);
          }

          // now we re-map original items to their result
          const okItems: TRes[] = [];
          for (const item of items) {
            const targets = item.targets;
            let targetMap = targetsByParent.get(item.obj);
            // make sure the sub pipeline returned at least one result for this item
            if (!targetMap && targets.length > 0) {
              logger.error({ msg: "Missing sub items", data: { item, targets, targetsByParent } });
              throw new ProgrammerError("Missing sub item");
            } else if (!targetMap) {
              targetMap = new Map();
            }

            // make sure the sub pipeline returned at least one result for this target
            const targetResults = new Map();
            let hasError = false;
            for (const target of targets) {
              if (!targetMap.has(target)) {
                logger.error({ msg: "Missing sub item target", data: { item, target, targetsByParent } });
                throw new ProgrammerError("Missing sub item target");
              }
              const targetRes = targetMap.get(target);
              if (!targetRes || !targetRes.success) {
                logger.debug({ msg: "Sub item failed", data: { item, targets, targetsByParent } });
                hasError = true;
              } else {
                targetResults.set(target, targetRes.result);
              }
            }
            if (!hasError) {
              okItems.push(options.formatOutput(item.obj, targetResults));
            } else {
              options.ctx.emitErrors(item.obj);
            }
          }

          return okItems;
        }),
      );
    }, options.ctx.streamConfig.workConcurrency),

    Rx.mergeAll(), // flatten items
  );
}
