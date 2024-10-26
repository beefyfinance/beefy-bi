import { rangeCovering, rangeExcludeMany, rangeMerge } from "../utils/range";

const ranges = [
  { from: 111148617, to: 127177251 },
  { from: 127177280, to: 127177282 },
  { from: 127177311, to: 127177317 },
  { from: 127177342, to: 127177345 },
  { from: 127177374, to: 127177377 },
  { from: 127177406, to: 127177408 },
  { from: 127177436, to: 127177441 },
  { from: 127177468, to: 127177471 },
  { from: 127177500, to: 127177504 },
  { from: 127177532, to: 127177534 },
  { from: 127177566, to: 127177566 },
  { from: 127177595, to: 127177597 },
  { from: 127177626, to: 127177629 },
  { from: 127177658, to: 127177661 },
  { from: 127177690, to: 127177692 },
  { from: 127177721, to: 127177723 },
  { from: 127177752, to: 127177755 },
  { from: 127177784, to: 127177786 },
  { from: 127177815, to: 127177818 },
  { from: 127177846, to: 127177849 },
  { from: 127177878, to: 127177881 },
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
