import S from "fluent-json-schema";

export const priceTypeSchema = S.string()
    .enum(["share_to_underlying", "underlying_to_usd"])
    .description("`share_to_underlying` is the ratio from share token to LP token (ppfs). `underlying_to_usd` is the ratio from an LP token to USD.");

export type PriceType = "share_to_underlying" | "underlying_to_usd"