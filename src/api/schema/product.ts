import S from "fluent-json-schema";

export const productKeySchema = S.string().minLength(10).maxLength(100);
