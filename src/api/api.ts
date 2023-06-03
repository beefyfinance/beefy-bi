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
import privateRoutes from "./route/private";
import publicRoutes from "./route/public";
import { registerDI } from "./service"; // register DI services

const logger = rootLogger.child({ module: "api", component: "main" });

type ServerOptions = { registerDI: (instance: FastifyInstance) => Promise<void>; rateLimit: RateLimitPluginOptions };
const optionsDefaults: ServerOptions = {
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
};

/**
 * Add all plugins to fastify
 * Make dependecy injection configurable for testing
 */
export async function buildPublicApi(options: ServerOptions = optionsDefaults) {
  const server = fastify({
    logger,
    trustProxy: true, // cloudflare or nginx http termination
  });

  await options.registerDI(server);

  await server
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
    .register(publicRoutes, { prefix: "/api/v1" });

  return server;
}

export async function buildPrivateApi(options: ServerOptions = optionsDefaults) {
  const server = fastify({
    logger,
    trustProxy: true, // cloudflare or nginx http termination
  });

  await options.registerDI(server);
  await server
    .register(FastifyPostgres, { connectionString: TIMESCALEDB_URL, application_name: "api" })
    .register(FastifyDI.fastifyAwilixPlugin)
    .register(FastifyUnderPressure)
    // cors and just helmet just in case these are ever exposed
    .register(FastifyCors, { origin: [API_URL, API_FRONTEND_URL, APP_PR_BUILDS_URL, APP_LOCAL_BUILDS_URL] })
    .register(FastifyHelmet, { contentSecurityPolicy: API_DISABLE_HTTPS ? false : true })
    // generous rate limit for private endpoints
    .register(FastifyRateLimit, {
      global: true,
      max: 100,
      timeWindow: 1000,
      cache: 10000,
      continueExceeding: true,
      skipOnError: false,
      enableDraftSpec: true, // default false. Uses IEFT draft header standard
    })
    .register(privateRoutes, { prefix: "/api/v1" });

  return server;
}
