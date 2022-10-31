import { diContainer } from "@fastify/awilix";
import { asClass, asValue, Lifetime } from "awilix";
import { getPgPool } from "../../utils/db";
import { getRedisClient } from "../../utils/shared-resources/shared-lock";
import { AsyncCache } from "./cache";
import { InvestorService } from "./investor";
import { PortfolioService } from "./portfolio";
import { PriceService } from "./price";
import { ProductService } from "./product";

const AbstractCache: any = require("abstract-cache"); // todo: add or install types

declare module "@fastify/awilix" {
  interface Cradle {
    db: Awaited<ReturnType<typeof getPgPool>>;
    investor: InvestorService;
    portfolio: PortfolioService;
    product: ProductService;
    price: PriceService;
    cache: AsyncCache;
  }
}

export async function registerDI() {
  const pgClient = await getPgPool({ freshClient: true, readOnly: true });
  const redisClient = await getRedisClient();
  const abcache = AbstractCache({
    useAwait: false,
    driver: {
      name: "abstract-cache-redis",
      options: { client: redisClient },
    },
  });
  const cache = new AsyncCache({ abcache: abcache });

  diContainer.register({
    db: asValue(pgClient),
    investor: asClass(InvestorService, {
      lifetime: Lifetime.SINGLETON,
    }),
    portfolio: asClass(PortfolioService, {
      lifetime: Lifetime.SINGLETON,
    }),
    product: asClass(ProductService, {
      lifetime: Lifetime.SINGLETON,
    }),
    price: asClass(PriceService, {
      lifetime: Lifetime.SINGLETON,
    }),
    cache: asValue(cache),
  });
}
