import FastifySwagger from "@fastify/swagger";
import FastifySwaggerUI from "@fastify/swagger-ui";
import { FastifyInstance } from "fastify";
import { API_DISABLE_HTTPS } from "../../../utils/config";

import { merge } from "lodash";
import { allChainIds } from "../../../types/chain";
import { ProductService } from "../../service/product";
import { BeefyPortfolioService } from "../../service/protocol/beefy";
import importStateRoutes from "./import-state";
import partnerRoutes from "./partner/linea";
import pricesRoutes from "./prices";
import beefyRoutes from "./protocol/beefy";

export default async function (instance: FastifyInstance) {
  const mergedComponents = merge(
    {
      ChainEnum: { $id: "ChainEnum", type: "string", enum: allChainIds, description: "The chain identifier" },
    },
    ProductService.schemaComponents,
    BeefyPortfolioService.timelineSchemaComponents,
    BeefyPortfolioService.investorCountsSchemaComponents,
  );

  for (const component of Object.values(mergedComponents)) {
    instance.addSchema(component);
  }

  instance
    .register(FastifySwagger, {
      openapi: {
        info: {
          title: "API",
          version: "0.0.1",
        },
      },
    })
    .register(FastifySwaggerUI, {
      uiConfig: {
        deepLinking: false,
      },
      staticCSP: API_DISABLE_HTTPS ? false : true,
    })
    .get("/openapi.json", { config: { rateLimit: false } }, (req, reply) => {
      reply.send(instance.swagger());
    })
    .register(beefyRoutes, { prefix: "/beefy" })
    .register(pricesRoutes, { prefix: "/price" })
    .register(importStateRoutes, { prefix: "/import-state" })
    .register(partnerRoutes, { prefix: "/partner" });
}
