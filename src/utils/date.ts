// Git's strict ISO format (`%aI`, what simple-git asks for) is
// `2016-06-15T14:13:20+02:00`, and since Git 2.45 a UTC timestamp is
// `2016-06-15T14:13:20Z` rather than `+00:00`. The previous hand-rolled
// parser treated a missing fractional-seconds group as NaN milliseconds
// and produced an Invalid Date, which threw in `toISOString`.
export function isoStringToDate(isoString: string): Date {
  return new Date(isoString);
}
