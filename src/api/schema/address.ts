import S from "fluent-json-schema";

export const addressSchema = S.string()
  .minLength(42)
  .maxLength(42)
  .pattern(/^0x[a-fA-F0-9]{40}$/);
