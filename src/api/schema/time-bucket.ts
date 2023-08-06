import S from "fluent-json-schema";
import { SamplingPeriod } from "../../types/sampling";

// <bucket_size>_<time_range>
export type TimeBucket = "1h_1d" | "1h_1w" | "1h_1M" | "4h_3M" | "1d_1M" | "1d_1Y" | "1d_all";
const timeBucketValues: TimeBucket[] = ["1h_1d", "1h_1w", "1h_1M", "4h_3M", "1d_1M", "1d_1Y", "1d_all"];
export const timeBucketSchema = S.string()
  .enum(timeBucketValues)
  .description(
    "Defines a bucket size and a time range like `<bucket size>_<time range>`. For example, `1d_1M` means: 1 month of data aggregated by buckets of 1 day.",
  )
  .required();

export function timeBucketToSamplingPeriod(timeBucket: TimeBucket) {
  const bucketParamMap: { [key in TimeBucket]: { bucketSize: SamplingPeriod; timeRange: SamplingPeriod } } = {
    "1h_1d": { bucketSize: "1hour", timeRange: "1day" },
    "1h_1w": { bucketSize: "1hour", timeRange: "1week" },
    "1h_1M": { bucketSize: "1hour", timeRange: "1month" },
    "1d_1M": { bucketSize: "1day", timeRange: "1month" },
    "4h_3M": { bucketSize: "4hour", timeRange: "3months" },
    "1d_1Y": { bucketSize: "1day", timeRange: "1year" },
    "1d_all": { bucketSize: "1day", timeRange: "100year" },
  };
  return bucketParamMap[timeBucket];
}
