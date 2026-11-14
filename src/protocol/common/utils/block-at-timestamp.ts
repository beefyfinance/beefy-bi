export interface BlockRef {
  number: number;
  timestamp: number; // unix seconds
}

// a point in time where the chain changed how fast it produces blocks
export interface BlockTimeChange {
  timestamp: number; // unix seconds, blocks after this are produced at the current rate
  secondsPerBlockBefore: number;
}

export interface BlockTimeModel {
  // seconds per block after the change (or the whole time if there is no change), measured on the chain
  secondsPerBlock: number;
  change?: BlockTimeChange;
}

function secondsPerBlockAt(model: BlockTimeModel, timestamp: number): number {
  if (model.change && timestamp <= model.change.timestamp) {
    return model.change.secondsPerBlockBefore;
  }
  return model.secondsPerBlock;
}

/**
 * Estimate the block number at targetTimestamp, going back from the latest block.
 * The walk goes through the block time change if there is one, so a window that spans the change is estimated correctly.
 */
export function estimateBlockNumber(options: { latest: BlockRef; targetTimestamp: number; model: BlockTimeModel }): number {
  const { latest, targetTimestamp, model } = options;

  let blocksBack = 0;
  let cursor = latest.timestamp;

  // the part of the window that is after the change, at the current rate
  if (model.change && model.change.timestamp < cursor && targetTimestamp < model.change.timestamp) {
    blocksBack += (cursor - model.change.timestamp) / model.secondsPerBlock;
    cursor = model.change.timestamp;
  }

  // the rest of the window, in the era the cursor is in
  if (cursor > targetTimestamp) {
    blocksBack += (cursor - targetTimestamp) / secondsPerBlockAt(model, cursor);
  }

  return clamp(Math.round(latest.number - blocksBack), 0, latest.number);
}

/**
 * Find a block whose timestamp is close to targetTimestamp.
 * The first guess comes from estimateBlockNumber, then each probe corrects the guess
 * with the rate of the era the probed block is in.
 * Returns the last probed block, so its number and timestamp always match.
 */
export async function findBlockNearTimestamp(options: {
  latest: BlockRef;
  targetTimestamp: number;
  model: BlockTimeModel;
  getBlock: (blockNumber: number) => Promise<BlockRef>;
  maxCorrections: number;
  toleranceSeconds: number;
}): Promise<BlockRef> {
  const { latest, targetTimestamp, model, getBlock, maxCorrections, toleranceSeconds } = options;

  let block = await getBlock(estimateBlockNumber({ latest, targetTimestamp, model }));
  for (let correction = 0; correction < maxCorrections; correction++) {
    const errorSeconds = targetTimestamp - block.timestamp; // positive: the block is before the target
    if (Math.abs(errorSeconds) <= toleranceSeconds) {
      break;
    }
    const nextNumber = clamp(Math.round(block.number + errorSeconds / secondsPerBlockAt(model, block.timestamp)), 0, latest.number);
    if (nextNumber === block.number) {
      break;
    }
    block = await getBlock(nextNumber);
  }
  return block;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
