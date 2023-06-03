import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { SamplingPeriod } from "../../../types/sampling";
import { dateTimeSchema } from "../../schema/datetime";
import { shortSamplingPeriodSchema } from "../../schema/enums";
import { PriceService } from "../../service/price";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  const schema = {
    querystring: S.object()
      .prop("oracle_id", S.string().minLength(1).maxLength(20).required())
      .prop("utc_datetime", dateTimeSchema.examples(["2023-01-01T00:00:00"]).required())
      .prop("look_around", shortSamplingPeriodSchema.default("1day"))
      .prop("half_limit", S.number().minimum(1).maximum(100).default(1)),
    tags: ["price"],
    summary: "Find the prices closest to a given date",
    description:
      "Fetches `half_limit` prices before and after the given `utc_datetime` for the given `oracle_id` at a maximum of `look_around` from the date. Only returns prices already in the database.",
    response: {
      200: PriceService.pricesAroundADateResponseSchema,
    },
  };
  type TRoute = {
    Querystring: {
      oracle_id: string;
      utc_datetime: string;
      look_around: SamplingPeriod;
      half_limit: number;
    };
  };

  await instance.get<TRoute>("/around-a-date", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
    const { oracle_id, utc_datetime, half_limit, look_around } = req.query;

    let datetime: Date;
    try {
      datetime = new Date(utc_datetime);
    } catch (e) {
      return reply.code(400).send({ error: "Could not parse date" });
    }

    const response = await instance.diContainer.cradle.price.getPricesAroundADate(oracle_id, datetime, look_around, half_limit);
    if (!response) {
      return reply.code(404).send({ error: "Price feed not found" });
    }
    return reply.send(response);
  });
}
