import { SupportedRangeTypes } from "../../../../utils/range";
import { QueryOptimizerOutput, SnapshotQueryOptimizerInput } from "./query-types";

export function optimizeSnapshotQueries<TObj, TRange extends SupportedRangeTypes>(
  input: SnapshotQueryOptimizerInput<TObj, TRange>,
): QueryOptimizerOutput<TObj, TRange>[] {
  return [];
}
