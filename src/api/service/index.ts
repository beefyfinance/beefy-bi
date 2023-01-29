import { diContainer } from "@fastify/awilix";
import { asClass, asFunction, asValue, Lifetime } from "awilix";
import { FastifyInstance } from "fastify";
import { DbClient } from "../../utils/db";
import { ProgrammerError } from "../../utils/programmer-error";
import { getRedisClient } from "../../utils/shared-resources/shared-lock";
import { AsyncCache } from "./cache";
import { InvestorService } from "./investor";
import { PortfolioService } from "./portfolio";
import { PriceService } from "./price";
import { ProductService } from "./product";
import { BeefyPortfolioService } from "./protocol/beefy";

const AbstractCache: any = require("abstract-cache"); // todo: add or install types

declare module "@fastify/awilix" {
  interface Cradle {
    db: DbClient;
    investor: InvestorService;
    portfolio: PortfolioService;
    beefy: BeefyPortfolioService;
    product: ProductService;
    price: PriceService;
    cache: AsyncCache;
  }
}

export async function registerDI(instance: FastifyInstance) {
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
    db: asFunction(
      () => {
        return {
          connect: () => {
            throw new ProgrammerError("Not implemented");
          },
          on: () => {
            throw new ProgrammerError("Not implemented");
          },
          end: () => {
            throw new ProgrammerError("Not implemented");
          },
          query: instance.pg.query.bind(instance.pg),
        } as DbClient;
      },
      { lifetime: Lifetime.TRANSIENT },
    ),
    investor: asClass(InvestorService, {
      lifetime: Lifetime.SINGLETON,
    }),
    portfolio: asClass(PortfolioService, {
      lifetime: Lifetime.SINGLETON,
    }),
    beefy: asClass(BeefyPortfolioService, {
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
