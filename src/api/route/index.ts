import { FastifyInstance, FastifyPluginOptions } from "fastify";

import portfolioRoutes from "./portfolio";

export default function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  instance.register(portfolioRoutes, { prefix: "/portfolio" });
  done();
}
