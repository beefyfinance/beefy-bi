import S from "fluent-json-schema";
import { SamplingPeriod } from "../../types/sampling";
import { ProgrammerError } from "../../utils/programmer-error";

// <bucket_size>_<time_range>
export type TimeBucket = "1h_1d" | "1h_1w" | "1d_1M" | "1d_1Y" | "1d_all";
export const timeBucketValues: TimeBucket[] = ["1h_1d", "1h_1w", "1d_1M", "1d_1Y", "1d_all"];
export const timeBucketSchema = S.string().enum(timeBucketValues).required();

export function timeBucketToSamplingPeriod(timeBucket: TimeBucket) {
  const bucketParamMap: { [key in TimeBucket]: { bucketSize: SamplingPeriod; timeRange: SamplingPeriod } } = {
    "1h_1d": { bucketSize: "1hour", timeRange: "1day" },
    "1h_1w": { bucketSize: "1hour", timeRange: "1week" },
    "1d_1M": { bucketSize: "1day", timeRange: "1month" },
    "1d_1Y": { bucketSize: "1day", timeRange: "1year" },
    "1d_all": { bucketSize: "1day", timeRange: "100year" },
  };
  return bucketParamMap[timeBucket];
}

export function assertIsValidTimeBucket(bucketSize: SamplingPeriod, timeRange: SamplingPeriod) {
  const isValidCombination =
    (bucketSize === "1hour" && timeRange === "1day") ||
    (bucketSize === "1hour" && timeRange === "1week") ||
    (bucketSize === "1day" && timeRange === "1month") ||
    (bucketSize === "1day" && timeRange === "1year") ||
    (bucketSize === "1day" && timeRange === "100year");
  if (!isValidCombination) {
    throw new ProgrammerError("Invalid bucketSize and timeRange combination");
  }
}
