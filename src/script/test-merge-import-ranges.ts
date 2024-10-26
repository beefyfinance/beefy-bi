import { rangeCovering, rangeExcludeMany, rangeMerge } from "../utils/range";

const ranges = [
  { from: 111148617, to: 127178086 },
  { from: 127178181, to: 127178190 },
];

const covering = rangeCovering(ranges);
const missingSpots = rangeExcludeMany(covering, rangeMerge(ranges));
const missingSpotsWithSize = missingSpots.map((r) => ({ ...r, length: r.to - r.from }));

const withDistanceFromPrevious = missingSpotsWithSize.map((r, i) => {
  if (i === 0) {
    return r;
  }
  return { ...r, dst: r.from - missingSpotsWithSize[i - 1].to };
});

console.log(withDistanceFromPrevious);

const min = withDistanceFromPrevious.reduce((acc, r) => Math.min(acc, Math.min(r.from, r.to)), Number.MAX_SAFE_INTEGER);
const max = withDistanceFromPrevious.reduce((acc, r) => Math.max(acc, Math.max(r.from, r.to)), Number.MIN_SAFE_INTEGER);
console.log({ min, max, diff: max - min });
