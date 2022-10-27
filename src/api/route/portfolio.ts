import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { addressSchema } from "../schema/address";

export default function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const schema = {
    querystring: S.object().prop("address", addressSchema),
  };
  instance.get<{
    Querystring: { address: string };
  }>("/", { schema }, async (req, reply) => {
    const { address } = req.query;
    const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
    if (investorId === null) {
      return reply.code(404).send({ error: "Investor not found" });
    }
    const productValues = await instance.diContainer.cradle.portfolio.getInvestorPortfolioValue(investorId);
    return reply.send(productValues);
  });

  done();
}
