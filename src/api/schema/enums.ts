import S from "fluent-json-schema";
import { allChainIds } from "../../types/chain";
import { allSamplingPeriods } from "../../types/sampling";

// <bucket_size>_<time_range>
export const shortSamplingPeriodSchema = S.string().enum(allSamplingPeriods.filter((p) => !p.includes("month") && !p.includes("year")));
export const chainSchema = S.string().enum(allChainIds);
