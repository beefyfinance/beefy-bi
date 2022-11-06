import { get, sum } from "lodash";
import * as Rx from "rxjs";
import { rootLogger } from "../../../utils/logger";
import { ProgrammerError } from "../../../utils/programmer-error";
import { ErrorEmitter, ImportCtx } from "../types/import-context";

const logger = rootLogger.child({ module: "common", component: "execute-sub-pipeline" });

/**
 * An rjxs operator that executes a sub pipeline.
 * Each value can yield an array that get passed to the sub pipeline.
 * If any error is yielded from any value of the sub pipeline, the current value is consided as failed.
 * Do cross value batching as much as possible
 */
export function executeSubPipeline$<TObj, TErr extends ErrorEmitter<TObj>, TRes, TSubTarget, TSubPipelineRes>(options: {
  ctx: ImportCtx;
  emitError: TErr;
  getObjs: (item: TObj) => TSubTarget[];
  pipeline: (
    emitError: ErrorEmitter<{ target: TSubTarget; parent: TObj }>,
  ) => Rx.OperatorFunction<{ target: TSubTarget; parent: TObj }, { target: TSubTarget; parent: TObj; result: TSubPipelineRes }>;
  formatOutput: (obj: TObj, subPipelineResult: Map<TSubTarget, TSubPipelineRes>) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  type ItemOkRes = { target: TSubTarget; parent: TObj; success: true; result: TSubPipelineRes };
  type ItemKoRes = { target: TSubTarget; parent: TObj; success: false };
  type ItemRes = ItemOkRes | ItemKoRes;

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

    Rx.concatMap((items) => {
      const subPipelineErrors: ItemKoRes[] = [];

      const emitError = (item: { target: TSubTarget; parent: TObj }) => {
        subPipelineErrors.push({ ...item, success: false });
      };

      const inputs = items.flatMap((item) => item.targets.map((target) => ({ target, parent: item.obj })));
      return Rx.of(inputs).pipe(
        Rx.mergeAll(),

        Rx.tap((item) => logger.trace({ msg: "importing sub-item", data: item })),

        // execute the sub pipeline
        options.pipeline(emitError),

        // return to product representation
        Rx.toArray(),
        Rx.map((partialSubItems) => {
          const subItems = (subPipelineErrors as ItemRes[]).concat(partialSubItems.map((item): ItemOkRes => ({ ...item, success: true })));

          logger.trace({ msg: "imported sub-items", data: { itemCount: items.length, subItems: subItems.length } });
          // make sure every sub item is present
          // if not, we consider the whole item as failed
          // this is to make sure we don't miss any error
          type MapRes = { success: true; result: TSubPipelineRes } | { success: false };
          const targetsByParent = new Map<TObj, Map<TSubTarget, MapRes>>();
          for (const subItem of subItems) {
            if (!targetsByParent.has(subItem.parent)) {
              targetsByParent.set(subItem.parent, new Map());
            }
            const res: MapRes = subItem.success ? { success: true, result: subItem.result } : { success: false };
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
              options.emitError(item.obj);
            }
          }

          return okItems;
        }),
      );
    }),

    Rx.mergeAll(), // flatten items
  );
}
