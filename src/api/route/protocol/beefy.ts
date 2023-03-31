import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { addressSchema } from "../../schema/address";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const schema = { querystring: S.object().prop("address", addressSchema.required()) };
  type TRoute = { Querystring: { address: string } };

  instance.get<TRoute>("/timeline", { schema }, async (req, reply) => {
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
