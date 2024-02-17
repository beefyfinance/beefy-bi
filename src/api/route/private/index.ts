import fastifyAuth from "@fastify/auth";
import fastifyBearerAuth from "@fastify/bearer-auth";
import fastifySwagger from "@fastify/swagger";
import fastifySwaggerUi from "@fastify/swagger-ui";
import { FastifyInstance } from "fastify";
import { merge } from "lodash";
import { allChainIds } from "../../../types/chain";
import { API_DISABLE_HTTPS, API_PRIVATE_TOKENS } from "../../../utils/config";
import { BeefyVaultService } from "../../service/protocol/beefy-vault";
import blockRoutes from "./blocks";
import priceRoutes from "./prices";
import beefyRoutes from "./protocol/beefy";

export default async function (instance: FastifyInstance) {
  const keys = new Set(API_PRIVATE_TOKENS);

  const mergedComponents = merge(
    {
      ChainEnum: { $id: "ChainEnum", type: "string", enum: allChainIds, description: "The chain identifier" },
    },
    BeefyVaultService.investorBalanceSchemaComponents,
  );

  for (const component of Object.values(mergedComponents)) {
    instance.addSchema(component);
  }

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
        tags: [
          { name: "price", description: "Price related end-points" },
          { name: "block", description: "Block related end-points" },
        ],
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
        docExpansion: "list",
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
  await instance
    .register(blockRoutes, { prefix: "/block", routeOpts })
    .register(priceRoutes, { prefix: "/price", routeOpts })
    .register(beefyRoutes, { prefix: "/beefy", routeOpts });
}
