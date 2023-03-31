import FastifyDI, { diContainer } from "@fastify/awilix";
import FastifyCaching from "@fastify/caching";
import FastifyCors from "@fastify/cors";
import FastifyHelmet from "@fastify/helmet";
import FastifyPostgres from "@fastify/postgres";
import FastifyRateLimit, { RateLimitPluginOptions } from "@fastify/rate-limit";
import FastifyUnderPressure from "@fastify/under-pressure";
import fastify, { FastifyInstance } from "fastify";
import { API_DISABLE_HTTPS, API_FRONTEND_URL, API_URL, APP_LOCAL_BUILDS_URL, APP_PR_BUILDS_URL, TIMESCALEDB_URL } from "../utils/config";
import { rootLogger } from "../utils/logger";
import routes from "./route";
import { registerDI } from "./service"; // register DI services

const logger = rootLogger.child({ module: "api", component: "main" });

/**
 * Add all plugins to fastify
 * Make dependecy injection configurable for testing
 */
export function buildApi(
  options: { registerDI: (instance: FastifyInstance) => Promise<void>; rateLimit: RateLimitPluginOptions } = {
    registerDI,
    rateLimit: {
      global: true,
      max: 25,
      timeWindow: 5000,
      cache: 10000,
      continueExceeding: true,
      skipOnError: false,
      enableDraftSpec: true, // default false. Uses IEFT draft header standard
    },
  },
) {
  const server = fastify({
    logger,
    trustProxy: true, // cloudflare or nginx http termination
  });

  // register anything that connects to redis (caching mostly)
  server.register(async (instance, opts, done) => {
    await options.registerDI(instance);

    instance
      //.register(FastifyRedis, { client: diContainer.cradle.redis })
      .register(FastifyPostgres, { connectionString: TIMESCALEDB_URL, application_name: "api" })
      .register(FastifyDI.fastifyAwilixPlugin)
      .register(FastifyUnderPressure)
      .register(FastifyCors, { origin: [API_URL, API_FRONTEND_URL, APP_PR_BUILDS_URL, APP_LOCAL_BUILDS_URL] })
      .register(FastifyHelmet, { contentSecurityPolicy: API_DISABLE_HTTPS ? false : true })
      .register(FastifyCaching, {
        privacy: FastifyCaching.privacy.PUBLIC,
        expiresIn: 1 * 60,
        cache: diContainer.cradle.abCache,
        serverExpiresIn: 1 * 60,
        etagMaxLife: 1 * 60,
      })
      // remove cache headers when the response is not 200 (400, 429, 404, etc)
      .addHook("onSend", async (req, reply) => {
        if (reply.raw.statusCode !== 200) {
          reply.header("cache-control", "no-cache");
        }
      })
      .register(FastifyRateLimit, {
        global: true,
        ...options.rateLimit,
      })
      .register(routes, { prefix: "/api/v1" });
    done();
  });

  return server;
}
