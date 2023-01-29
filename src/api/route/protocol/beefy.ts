import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { addressSchema } from "../../schema/address";
import { getRateLimitOpts } from "../../utils/rate-limiter";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const schema = { querystring: S.object().prop("address", addressSchema.required()) };
  type TRoute = { Querystring: { address: string } };
  const routeOptions = { schema, config: { rateLimit: await getRateLimitOpts() } };

  instance.get<TRoute>("/", routeOptions, async (req, reply) => {
    const { address } = req.query;
    const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
    if (investorId === null) {
      return reply.code(404).send({ error: "Investor not found" });
    }
    const productValues = await instance.diContainer.cradle.beefy.getInvestorPortfolioValue(investorId);
    return reply.send(productValues);
  });

  instance.get<TRoute>("/timeline", routeOptions, async (req, reply) => {
    const { address } = req.query;
    const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
    if (investorId === null) {
      return reply.code(404).send({ error: "Investor not found" });
    }
    const investorTimeline = await instance.diContainer.cradle.beefy.getInvestorTimeline(investorId);
    return reply.send(investorTimeline);
  });

  done();
}
