import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  {
    const schema = {
      params: S.object().prop("block_number", S.number().required().description("The block number to fetch the balances at")),
      querystring: S.object()
        .prop("block_datetime", S.string().format("date-time").description("The block datetime. Can be provided to speed up this endpoint."))
        .prop("block_timestamp", S.number().description("The block timestamp (utc epoch. Can be provided to speed up this endpoint.")),
      tags: ["partners"],
      summary: "Linea investor balances at block",
      description: "Fetches All Investor Balances for all Linea Products at a Specific Block.",
      response: {
        200: { content: { "text/csv": {} } },
      },
    };
    type TRoute = {
      Params: { block_number: number };
      Querystring: { block_datetime?: string; block_timestamp?: number };
    };

    instance.get<TRoute>("/linea/all_balances_at/:block_number", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { block_number } = req.params;
      const { block_datetime: block_datetime_str, block_timestamp } = req.query;

      if (block_datetime_str && block_timestamp) {
        return reply.code(400).send({ error: "Only provide either block_datetime or block_timestamp" });
      }

      const block_datetime = block_datetime_str ? new Date(block_datetime_str) : block_timestamp ? new Date(block_timestamp * 1000) : null;

      const data = await instance.diContainer.cradle.beefyVault.getLineaAllInvestorBalancesAtBlock(block_number, block_datetime);
      return reply.send(data);
    });
  }
}
