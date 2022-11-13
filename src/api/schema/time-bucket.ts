import S from "fluent-json-schema";
export const timeBucketSchema = S.string().enum(["15min", "1hour", "4hour", "1day", "1week"]).required();
