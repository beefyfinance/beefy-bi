import FastifySwagger from "@fastify/swagger";
import FastifySwaggerUI from "@fastify/swagger-ui";
import { FastifyInstance, FastifyPluginOptions } from "fastify";

import portfolioRoutes from "./portfolio";

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
    })
    .get("/openapi.json", { config: { rateLimit: false } }, (req, reply) => {
      reply.send(instance.swagger());
    });
  instance.register(portfolioRoutes, { prefix: "/portfolio" });
  done();
}
