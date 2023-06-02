import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { Chain } from "../../types/chain";
import { chainSchema } from "../schema/chain";
import { dateTimeSchema } from "../schema/datetime";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const schema = {
    querystring: S.object().prop("chain", chainSchema.required()).prop("utc_datetime", dateTimeSchema.required()),
  };
  type TRoute = {
    Querystring: {
      chain: Chain;
      utc_datetime: string;
    };
  };

  instance.get<TRoute>("/around-a-date", { schema }, async (req, reply) => {
    const { chain, utc_datetime } = req.query;

    let datetime: Date;
    try {
      datetime = new Date(utc_datetime);
    } catch (e) {
      return reply.code(400).send({ error: "Could not parse date" });
    }

    const blocks = await instance.diContainer.cradle.block.getBlockAroundADate(chain, datetime);
    return reply.send(blocks);
  });

  done();
}
