import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { Chain } from "../../../types/chain";
import { SamplingPeriod } from "../../../types/sampling";
import { dateTimeSchema } from "../../schema/datetime";
import { chainSchema, shortSamplingPeriodSchema } from "../../schema/enums";
import { BlockService } from "../../service/block";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  const schema = {
    querystring: S.object()
      .prop("chain", chainSchema.required())
      .prop("utc_datetime", dateTimeSchema.examples(["2023-01-01T00:00:00"]).required())
      .prop("look_around", shortSamplingPeriodSchema.default("1day"))
      .prop("half_limit", S.number().minimum(1).maximum(100).default(1)),
    tags: ["block"],
    summary: "Find the blocks closest to a given date",
    description:
      "Fetches `half_limit` blocks before and after the given `utc_datetime` for the given `chain` at a maximum of `look_around` from the date. Only returns blocks already in the database.",
    response: {
      200: BlockService.blocksAroundADateResponseSchema,
    },
  };
  type TRoute = {
    Querystring: {
      chain: Chain;
      utc_datetime: string;
      look_around: SamplingPeriod;
      half_limit: number;
    };
  };

  await instance.get<TRoute>("/around-a-date", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
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
