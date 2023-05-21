import { max, sortBy } from "lodash";
import { SamplingPeriod, samplingPeriodMs } from "../../../../types/sampling";
import { Range, rangeSplitManyToMaxLength } from "../../../../utils/range";

export function createOptimizerIndexFromBlockList({
  mode,
  blockNumberList,
  latestBlockNumber,
  snapshotInterval,
  maxBlocksPerQuery,
  msPerBlockEstimate,
}: {
  mode: "recent" | "historical";
  blockNumberList: number[];
  latestBlockNumber: number;
  maxBlocksPerQuery: number;
  snapshotInterval: SamplingPeriod;
  msPerBlockEstimate: number;
}): Range<number>[] {
  const blockRanges: Range<number>[] = [];

  const sortedBlockNumbers = sortBy(blockNumberList);
  for (let i = 0; i < sortedBlockNumbers.length - 1; i++) {
    const block = sortedBlockNumbers[i];
    const nextBlock = blockNumberList[i + 1];
    blockRanges.push({ from: block, to: nextBlock - 1 });
  }
  // add a range between last db block and latest block
  if (blockRanges.length === 0) {
    return [];
  }
  const maxDbBlock = max(blockNumberList) as number;

  if (latestBlockNumber > maxDbBlock) {
    blockRanges.push({ from: maxDbBlock + 1, to: latestBlockNumber });
  }

  // split ranges in chunks of ~15min
  const maxTimeStepMs = samplingPeriodMs[snapshotInterval];
  const avgBlockPerTimeStep = Math.floor(maxTimeStepMs / msPerBlockEstimate);
  const rangeMaxLength = Math.min(avgBlockPerTimeStep, maxBlocksPerQuery);

  let finalRanges = rangeSplitManyToMaxLength(blockRanges, rangeMaxLength);
  if (mode === "recent") {
    finalRanges = [finalRanges[finalRanges.length - 1]];
  }
  return finalRanges;
}
