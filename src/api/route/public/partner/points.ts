import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { Chain } from "../../../../types/chain";
import { chainSchema } from "../../../schema/enums";
import { BeefyVaultService } from "../../../service/protocol/beefy-vault";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  {
    const schema = {
      params: S.object()
        .prop("chain", chainSchema.required())
        .prop("platform_id", S.string().required().description("The platform id to fetch the balances for."))
        .prop("block_number", S.number().required().description("The block number to fetch the balances at")),
      querystring: S.object()
        .prop("block_datetime", S.string().format("date-time").description("The block datetime. Can be provided to speed up this endpoint."))
        .prop("block_timestamp", S.number().description("The block timestamp (utc epoch. Can be provided to speed up this endpoint.")),
      tags: ["partners"],
      summary: "Balances at block of products earning points",
      description: "Fetches All Investor Balances for all Beefy Products earning points at a Specific Block.",
      response: {
        200: BeefyVaultService.pointsBalancesSchema,
      },
    };
    type TRoute = {
      Params: { chain: Chain; platform_id: string; block_number: number };
      Querystring: { block_datetime?: string; block_timestamp?: number };
    };

    instance.get<TRoute>("/:chain/:platform_id/balances_at/:block_number", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, platform_id, block_number } = req.params;
      const { block_datetime: block_datetime_str, block_timestamp } = req.query;

      if (block_datetime_str && block_timestamp) {
        return reply.code(400).send({ error: "Only provide either block_datetime or block_timestamp" });
      }

      const block_datetime = block_datetime_str ? new Date(block_datetime_str) : block_timestamp ? new Date(block_timestamp * 1000) : null;

      const data = await instance.diContainer.cradle.beefyVault.getInvestorBalancesOfPlatformEarningPoints(
        chain,
        platform_id,
        block_number,
        block_datetime,
      );
      return reply.send(data);
    });
  }
}
