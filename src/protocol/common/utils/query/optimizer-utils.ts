import { isArray, keyBy } from "lodash";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, SupportedRangeTypes } from "../../../../utils/range";
import { AddressBatchOutput, JsonRpcBatchOutput, OptimizerInput, QueryOptimizerOutput, StrategyInput, StrategyResult } from "./query-types";

// some type guards and accessors
export function isJsonRpcBatchQueries<TObj, TRange extends SupportedRangeTypes>(
  o: QueryOptimizerOutput<TObj, TRange>,
): o is JsonRpcBatchOutput<TObj, TRange> {
  return o.type === "jsonrpc batch";
}
export function isAddressBatchQueries<TObj, TRange extends SupportedRangeTypes>(
  o: QueryOptimizerOutput<TObj, TRange>,
): o is AddressBatchOutput<TObj, TRange> {
  return o.type === "address batch";
}

export function extractObjsAndRangeFromOptimizerOutput<TObj, TRange extends SupportedRangeTypes>({
  output,
  objKey,
}: {
  objKey: (obj: TObj) => string;
  output: QueryOptimizerOutput<TObj, TRange>;
}): { obj: TObj; range: Range<TRange> }[] {
  if (isJsonRpcBatchQueries(output)) {
    return [{ obj: output.obj, range: output.range }];
  } else if (isAddressBatchQueries(output)) {
    const postfiltersByObj = keyBy(output.postFilters, (pf) => objKey(pf.obj));
    return output.objs.flatMap((obj) => {
      const postFilter = postfiltersByObj[objKey(obj)];
      if (postFilter && postFilter.filter !== "no-filter") {
        return postFilter.filter.map((range) => ({ obj: postFilter.obj, range }));
      } else {
        return { obj, range: output.range };
      }
    });
  } else {
    throw new ProgrammerError("Unsupported type of optimizer output: " + output);
  }
}

export function getLoggableOptimizerOutput<TObj, TRange extends SupportedRangeTypes>(
  input: OptimizerInput<TObj, TRange> | StrategyInput<TObj, TRange>,
  output: (QueryOptimizerOutput<TObj, TRange> | StrategyResult<QueryOptimizerOutput<TObj, TRange>>) | QueryOptimizerOutput<TObj, TRange>[],
): any {
  if (isArray(output)) {
    return output.map((o) => getLoggableOptimizerOutput(input, o));
  }
  if ("totalCoverage" in output) {
    return { ...output, result: getLoggableOptimizerOutput(input, output.result) };
  }
  if (isAddressBatchQueries(output)) {
    return {
      ...output,
      objs: output.objs.map(input.objKey),
      postFilters: output.postFilters.map((f) => ({ ...f, obj: input.objKey(f.obj) })),
    };
  } else {
    return { ...output, obj: input.objKey(output.obj) };
  }
}

export function getLoggableInput<TObj, TRange extends SupportedRangeTypes>(input: OptimizerInput<TObj, TRange> | StrategyInput<TObj, TRange>): any {
  return { ...input, states: input.states.map((s) => ({ ...s, obj: input.objKey(s.obj) })) };
}
