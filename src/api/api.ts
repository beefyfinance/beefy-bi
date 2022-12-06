import FastifyDI from "@fastify/awilix";
import FastifyCaching, { FastifyCachingPluginOptions } from "@fastify/caching";
import FastifyCors from "@fastify/cors";
import FastifyEtag from "@fastify/etag";
import FastifyHelmet from "@fastify/helmet";
import FastifyPostgres from "@fastify/postgres";
import FastifyRateLimit from "@fastify/rate-limit";
import FastifyRedis from "@fastify/redis";
import FastifyUnderPressure from "@fastify/under-pressure";
import fastify from "fastify";
import { API_DISABLE_HTTPS, API_FRONTEND_URL, API_LISTEN, API_PORT, API_URL, TIMESCALEDB_URL } from "../utils/config";
import { rootLogger } from "../utils/logger";
import { getRedisClient } from "../utils/shared-resources/shared-lock";
import routes from "./route";
import { registerDI } from "./service"; // register DI services

const logger = rootLogger.child({ module: "api", component: "main" });
const server = fastify({
  logger,
});

// register anything that connects to redis (caching mostly)
server.register(async (instance, opts, done) => {
  await registerDI(instance);
  const redisClient = await getRedisClient();
  const AbstractCache: any = require("abstract-cache"); // todo: add or install types
  const abcache = AbstractCache({
    useAwait: false,
    driver: {
      name: "abstract-cache-redis", // must be installed via `npm i`
      options: { client: redisClient },
    },
  });
  const cacheOptions: FastifyCachingPluginOptions = {
    privacy: FastifyCaching.privacy.PUBLIC,
    expiresIn: 5 * 60,
    cache: abcache,
    serverExpiresIn: 5 * 60,
    etagMaxLife: 5 * 60,
  };

  instance
    .register(FastifyRedis, { client: redisClient })
    .register(FastifyPostgres, {
      connectionString: TIMESCALEDB_URL,
      application_name: "api",
    })
    .register(FastifyDI.fastifyAwilixPlugin)
    .register(FastifyUnderPressure)
    // rate limit disabled globally because I don't know how to disable it just for swagger ui
    .register(FastifyRateLimit, { global: false })
    .register(FastifyCors, { origin: [API_URL, API_FRONTEND_URL] })
    .register(FastifyHelmet, { contentSecurityPolicy: API_DISABLE_HTTPS ? false : true })
    .register(FastifyEtag)
    .register(FastifyCaching, cacheOptions)
    .register(routes, { prefix: "/api/v0" });

  done();
});

server.listen({ port: API_PORT, host: API_LISTEN }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening at ${address}`);
});
