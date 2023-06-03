import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { Chain } from "../../types/chain";
import { SamplingPeriod } from "../../types/sampling";
import { dateTimeSchema } from "../schema/datetime";
import { chainSchema, samplingPeriodSchema } from "../schema/enums";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  const schema = {
    querystring: S.object()
      .prop("chain", chainSchema.required())
      .prop("utc_datetime", dateTimeSchema.required())
      .prop("look_around", samplingPeriodSchema.default("1day"))
      .prop("half_limit", S.number().minimum(1).maximum(100).default(10)),
  };
  type TRoute = {
    Querystring: {
      chain: Chain;
      utc_datetime: string;
      look_around: SamplingPeriod;
      half_limit: number;
    };
  };

  instance.get<TRoute>("/around-a-date", { ...opts.routeOpts, schema }, async (req, reply) => {
    const { chain, utc_datetime, half_limit, look_around } = req.query;

    let datetime: Date;
    try {
      datetime = new Date(utc_datetime);
    } catch (e) {
      return reply.code(400).send({ error: "Could not parse date" });
    }

    const blocks = await instance.diContainer.cradle.block.getBlockAroundADate(chain, datetime, look_around, half_limit);
    return reply.send(blocks);
  });
}
