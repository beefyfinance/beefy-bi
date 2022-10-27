import FastifyCaching, { FastifyCachingPluginOptions } from "@fastify/caching";
import FastifyCors from "@fastify/cors";
import FastifyEtag from "@fastify/etag";
import FastifyHelmet from "@fastify/helmet";
import FastifyPostgres from "@fastify/postgres";
import FastifyRateLimit from "@fastify/rate-limit";
import FastifyRedis from "@fastify/redis";
import { FastifyPluginAsyncJsonSchemaToTs } from "@fastify/type-provider-json-schema-to-ts";
import FastifyUnderPressure from "@fastify/under-pressure";
import fastify from "fastify";
import { TIMESCALEDB_URL } from "../utils/config";
import { rootLogger } from "../utils/logger";
import { getRedisClient } from "../utils/shared-resources/shared-lock";

const logger = rootLogger.child({ module: "api", component: "main" });
const server = fastify();

// register anything that connects to redis (caching mostly)
server.register(async (instance, opts, done) => {
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
    .register(FastifyUnderPressure)
    .register(FastifyRateLimit, {
      max: 1,
      timeWindow: 5000,
      cache: 10000,
      redis: redisClient,
      continueExceeding: true,
      skipOnError: false,
      enableDraftSpec: true, // default false. Uses IEFT draft header standard
    })
    .register(FastifyCors)
    .register(FastifyHelmet)
    .register(FastifyEtag)
    .register(FastifyCaching, cacheOptions);

  instance.get("/", (req, reply) => {
    instance.cache.set("hello", { hello: "world" }, 10000, (err) => {
      if (err) return reply.send(err);
      reply.send({ hello: "world" });
    });
  });

  instance.get("/cache", (req, reply) => {
    console.log("Headers: ", req.headers);
    instance.cache.get("hello", (err, val) => {
      reply.send(err || val);
    });
  });

  instance.get("/products", function (req, reply) {
    instance.pg.query("SELECT * from product limit 1", [], function onResult(err, result) {
      reply.send(err || result.rows);
    });
  });

  done();
});

server.listen({ port: 8080 }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening at ${address}`);
});
