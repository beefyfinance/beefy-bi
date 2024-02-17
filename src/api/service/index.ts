import { diContainer } from "@fastify/awilix";
import { AbstractCacheCompliantObject } from "@fastify/caching";
import { asClass, asFunction, asValue, Lifetime } from "awilix";
import { FastifyInstance } from "fastify";
import { Redis } from "ioredis";
import { DbClient } from "../../utils/db";
import { ProgrammerError } from "../../utils/programmer-error";
import { getRedisClient } from "../../utils/shared-resources/shared-lock";
import { BlockService } from "./block";
import { AsyncCache } from "./cache";
import { ImportStateService } from "./import-state";
import { InvestorService } from "./investor";
import { PriceService } from "./price";
import { ProductService } from "./product";
import { BeefyPortfolioService } from "./protocol/beefy";
import { BeefyApiService } from "./protocol/beefy-api";
import { BeefyVaultService } from "./protocol/beefy-vault";
import { RpcService } from "./rpc";

const AbstractCache: any = require("abstract-cache"); // todo: add or install types

declare module "@fastify/awilix" {
  interface Cradle {
    db: DbClient;
    investor: InvestorService;
    beefy: BeefyPortfolioService;
    product: ProductService;
    price: PriceService;
    block: BlockService;
    importState: ImportStateService;
    beefyApi: BeefyApiService;
    beefyVault: BeefyVaultService;
    abCache: AbstractCacheCompliantObject;
    rpc: RpcService;
    cache: AsyncCache;
    redis: Redis;
  }
}

export async function registerDI(instance: FastifyInstance) {
  const redisClient = await getRedisClient();
  const abCache = AbstractCache({
    useAwait: false,
    driver: {
      name: "abstract-cache-redis",
      options: { client: redisClient },
    },
  });
  const cache = new AsyncCache({ abCache: abCache });

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
    investor: asClass(InvestorService, { lifetime: Lifetime.SINGLETON }),
    beefy: asClass(BeefyPortfolioService, { lifetime: Lifetime.SINGLETON }),
    beefyApi: asClass(BeefyApiService, { lifetime: Lifetime.SINGLETON }),
    beefyVault: asClass(BeefyVaultService, { lifetime: Lifetime.SINGLETON }),
    product: asClass(ProductService, { lifetime: Lifetime.SINGLETON }),
    price: asClass(PriceService, { lifetime: Lifetime.SINGLETON }),
    block: asClass(BlockService, { lifetime: Lifetime.SINGLETON }),
    importState: asClass(ImportStateService, { lifetime: Lifetime.SINGLETON }),
    rpc: asClass(RpcService, { lifetime: Lifetime.SINGLETON }),
    cache: asValue(cache),
    abCache: asValue(abCache),
    redis: asValue(redisClient),
  });
}
