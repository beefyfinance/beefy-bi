import S from "fluent-json-schema";

export const addressSchema = S.string()
  .minLength(42)
  .maxLength(42)
  .pattern(/^0x[a-fA-F0-9]{40}$/)
  .examples([
    "0x5e1caC103F943Cd84A1E92dAde4145664ebf692A",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
  ]);
