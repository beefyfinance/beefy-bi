import S from "fluent-json-schema";
import { allChainIds } from "../../types/chain";

export const chainSchema = S.string().enum(allChainIds).required();
