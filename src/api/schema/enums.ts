import S from "fluent-json-schema";
import { allChainIds } from "../../types/chain";
import { allSamplingPeriods } from "../../types/sampling";

// <bucket_size>_<time_range>
export const samplingPeriodSchema = S.string().enum(allSamplingPeriods);
export const chainSchema = S.string().enum(allChainIds);
