import { samplingPeriodMs } from "../../../../types/sampling";
import { rootLogger } from "../../../../utils/logger";
import { ProgrammerError } from "../../../../utils/programmer-error";
import { Range, isValidRange, rangeMerge } from "../../../../utils/range";
import { DbBlockNumberRangeImportState } from "../../loader/import-state";
import { ImportBehaviour } from "../../types/import-context";

const logger = rootLogger.child({ module: "common", component: "import-state-to-range-input" });

/**
 * Apply the behaviour to the considered import state,
 * There is many options to apply:
 * - ignore the import state if needed
 * - skip the recent window if needed
 * - force a range to be considered
 * - max blocks per query
 * - max queries per batch
 */
export function importStateToOptimizerRangeInput({
  behaviour,
  maxBlocksPerQuery,
  msPerBlockEstimate,
  lastImportedBlockNumber,
  isLive,
  latestBlockNumber,
  importState,
}: {
  behaviour: ImportBehaviour;
  msPerBlockEstimate: number;
  maxBlocksPerQuery: number;
  latestBlockNumber: number;
  lastImportedBlockNumber: number | null;
  importState: DbBlockNumberRangeImportState | null;
  isLive: boolean;
}) {
  // compute recent full range in case we need it
  // fetch the last hour of data
  const period = samplingPeriodMs["1day"];
  const periodInBlockCountEstimate = Math.floor(period / msPerBlockEstimate);

  const diffBetweenLastImported = lastImportedBlockNumber
    ? Math.max(0, latestBlockNumber - (lastImportedBlockNumber + behaviour.waitForBlockPropagation + 1))
    : Infinity;

  const blockCountToFetch = Math.min(maxBlocksPerQuery, periodInBlockCountEstimate, diffBetweenLastImported);
  const fromBlock = Math.max(0 + behaviour.waitForBlockPropagation, latestBlockNumber - blockCountToFetch);
  const toBlock = latestBlockNumber;
  const recentFullRange = {
    from: fromBlock - behaviour.waitForBlockPropagation,
    to: toBlock - behaviour.waitForBlockPropagation,
  };

  let fullRange: Range<number>;

  if (behaviour.mode !== "recent" && importState !== null) {
    // exclude latest block query from the range
    const skipRecent = behaviour.skipRecentWindowWhenHistorical;
    let doSkip = false;
    if (skipRecent === "all") {
      doSkip = true;
    } else if (skipRecent === "none") {
      doSkip = false;
    } else if (skipRecent === "live") {
      doSkip = isLive;
    } else if (skipRecent === "eol") {
      doSkip = !isLive;
    } else {
      throw new ProgrammerError({ msg: "Invalid skipRecentWindowWhenHistorical value", data: { skipRecent } });
    }
    // this is the whole range we have to cover
    fullRange = {
      from: importState.importData.contractCreatedAtBlock,
      to: Math.min(latestBlockNumber - behaviour.waitForBlockPropagation, doSkip ? recentFullRange.to : Infinity),
    };
  } else {
    fullRange = recentFullRange;
  }

  // this can happen when we force the block number in the past and we are treating a recent product
  if (fullRange.from > fullRange.to) {
    const importStateKey = importState?.importKey;
    if (behaviour.forceConsideredBlockRange !== null) {
      logger.info({
        msg: "current block number set too far in the past to treat this product",
        data: { fullRange, importStateKey },
      });
    } else {
      logger.error({
        msg: "Full range is invalid",
        data: { fullRange, importStateKey },
      });
      if (process.env.NODE_ENV === "development") {
        throw new ProgrammerError("Full range is invalid");
      }
    }
  }

  const coveredRanges = behaviour.ignoreImportState ? [] : importState?.importData.ranges.coveredRanges || [];
  let toRetry = !behaviour.ignoreImportState && behaviour.mode === "historical" && importState !== null ? importState.importData.ranges.toRetry : [];

  // apply our range restriction everywhere
  if (behaviour.forceConsideredBlockRange !== null) {
    const restrict = behaviour.forceConsideredBlockRange;
    fullRange = {
      from: Math.max(fullRange.from, restrict.from),
      to: Math.min(fullRange.to, restrict.to),
    };
    toRetry = rangeMerge(
      toRetry
        .map((range) => ({
          from: Math.max(range.from, restrict.from),
          to: Math.min(range.to, restrict.to),
        }))
        .filter((r) => isValidRange(r)),
    );
  }

  return { fullRange, coveredRanges, toRetry };
}
