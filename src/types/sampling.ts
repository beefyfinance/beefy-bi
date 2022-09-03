export type SamplingPeriod = "1min" | "5min" | "15min" | "1hour" | "4hour" | "1day";
export const allSamplingPeriods = ["1min", "5min", "15min", "1hour", "4hour", "1day"];
export const samplingPeriodMs: { [period in SamplingPeriod]: number } = {
  "1min": 1 * 60 * 1000,
  "5min": 5 * 60 * 1000,
  "15min": 15 * 60 * 1000,
  "1hour": 60 * 60 * 1000,
  "4hour": 4 * 60 * 60 * 1000,
  "1day": 24 * 60 * 60 * 1000,
};
