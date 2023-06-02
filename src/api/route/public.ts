import FastifySwagger from "@fastify/swagger";
import FastifySwaggerUI from "@fastify/swagger-ui";
import { FastifyInstance, FastifyPluginOptions } from "fastify";
import { API_DISABLE_HTTPS } from "../../utils/config";

import importStateRoutes from "./import-state";
import pricesRoutes from "./prices";
import beefyRoutes from "./protocol/beefy";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
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
        docExpansion: "full",
        deepLinking: false,
      },
      staticCSP: API_DISABLE_HTTPS ? false : true,
    })
    .get("/openapi.json", { config: { rateLimit: false } }, (req, reply) => {
      reply.send(instance.swagger());
    });
  instance.register(beefyRoutes, { prefix: "/beefy" });
  instance.register(pricesRoutes, { prefix: "/price" });
  instance.register(importStateRoutes, { prefix: "/import-state" });
  done();
}
