import FastifySwagger from "@fastify/swagger";
import FastifySwaggerUI from "@fastify/swagger-ui";
import { FastifyInstance } from "fastify";
import { API_DISABLE_HTTPS } from "../../../utils/config";

import importStateRoutes from "./import-state";
import pricesRoutes from "./prices";
import beefyRoutes from "./protocol/beefy";

export default async function (instance: FastifyInstance) {
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
    .register(importStateRoutes, { prefix: "/import-state" });
}
