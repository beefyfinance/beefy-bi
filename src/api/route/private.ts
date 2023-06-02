import { FastifyInstance, FastifyPluginOptions } from "fastify";

import blockRoutes from "./blocks";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  instance.register(blockRoutes, { prefix: "/block" });
  done();
}
