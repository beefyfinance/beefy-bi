import { Chain } from "../types/chain";
import { logger } from "../utils/logger";
import axios from "axios";
import { BEEFY_DATA_URL } from "../utils/config";

interface PriceSnapshot {
  oracleId: string;
  datetime: Date;
  value: number;
}

export async function* streamBeefyPrices(
  chain: Chain,
  oracleId: string,
  options?: {
    startDate?: Date;
    endDate?: Date;
  }
) {
  const startDate = options?.startDate || new Date(0);
  const endDate = options?.endDate || new Date(new Date().getTime() + 10000000);
  logger.debug(`[VAULT_PRICE_STREAM] Fetching prices for ${chain}:${oracleId}`);
  const res = await axios.get(BEEFY_DATA_URL + "/price", {
    params: {
      name: oracleId,
      period: "minute",
      from: Math.floor(startDate.getTime() / 1000),
      to: Math.ceil(endDate.getTime() / 1000),
      limit: 1000000000,
    },
  });

  for (const price of res.data) {
    const event: PriceSnapshot = {
      datetime: new Date(price.ts),
      oracleId: price.name,
      value: price.v,
    };
    yield event;
  }
}
