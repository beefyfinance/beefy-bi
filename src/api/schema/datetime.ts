import S from "fluent-json-schema";

export const dateTimeSchema = S.string().examples(["2023-01-01T00:00:00"]).format("date-time");
