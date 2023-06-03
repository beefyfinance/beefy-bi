import fastifyAuth from "@fastify/auth";
import fastifyBearerAuth from "@fastify/bearer-auth";
import fastifySwagger from "@fastify/swagger";
import fastifySwaggerUi from "@fastify/swagger-ui";
import { FastifyInstance } from "fastify";
import { API_DISABLE_HTTPS, API_PRIVATE_TOKENS } from "../../utils/config";
import blockRoutes from "./blocks";

export default async function (instance: FastifyInstance) {
  const keys = new Set(API_PRIVATE_TOKENS);
  // await is needed here for the auth plugin to register properly
  await instance
    // authenticate
    .register(fastifyAuth)
    .register(fastifyBearerAuth, { keys, addHook: false })
    // register swagger outside of the protected routes
    .register(fastifySwagger, {
      openapi: {
        info: {
          title: "API",
          version: "0.0.1",
        },
        components: {
          securitySchemes: {
            bearerToken: {
              type: "http",
              scheme: "bearer",
            },
          },
        },
      },
    })
    .register(fastifySwaggerUi, {
      uiConfig: {
        docExpansion: "full",
        deepLinking: false,
      },
      staticCSP: API_DISABLE_HTTPS ? false : true,
    })
    .get("/openapi.json", { config: { rateLimit: false } }, (req, reply) => {
      reply.send(instance.swagger());
    });

  // register all protected routes
  // this needs to be done after the auth plugin is registered and awaited
  const routeOpts = { preHandler: instance.auth([instance.verifyBearerAuth as any]), schema: { security: [{ bearerToken: [] }] } };
  await instance.register(blockRoutes, {
    prefix: "/block",
    routeOpts,
  });
}
