/**
 * Find the first block where a contract has code, i.e. its deployment block.
 * Binary search over block numbers: once deployed, a contract keeps its code, so "has code at block n" is monotonic.
 * Returns null if the contract has no code at the latest block.
 */
export async function findFirstBlockWithCode(options: {
  latestBlockNumber: number;
  hasCodeAt: (blockNumber: number) => Promise<boolean>;
}): Promise<number | null> {
  const { latestBlockNumber, hasCodeAt } = options;

  if (!(await hasCodeAt(latestBlockNumber))) {
    return null;
  }

  // invariant: the contract has code at hi
  let lo = 0;
  let hi = latestBlockNumber;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (await hasCodeAt(mid)) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }
  return lo;
}
