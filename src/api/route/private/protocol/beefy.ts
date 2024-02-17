import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { Chain } from "../../../../types/chain";
import { addressSchema } from "../../../schema/address";
import { chainSchema } from "../../../schema/enums";
import { BeefyVaultService } from "../../../service/protocol/beefy-vault";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  {
    const schema = {
      params: S.object()
        .prop("chain", chainSchema.required().description("Only include products for this chain"))
        .prop("contract_address", addressSchema.required().description("The product contract address"))
        .prop("block_number", S.number().required().description("The block number to fetch the balances at")),
      querystring: S.object()
        .prop("block_datetime", S.string().format("date-time").description("The block datetime. Can be provided to speed up this endpoint."))
        .prop("block_timestamp", S.number().description("The block timestamp (utc epoch. Can be provided to speed up this endpoint.")),
      tags: ["beefy"],
      summary: "Fetch all investor balances for a specific product at a specific block",
      description: "This endpoint returns all investor balances for a specific product at a specific block",
      response: {
        200: BeefyVaultService.investorBalancesSchema,
      },
    };
    type TRoute = {
      Params: { chain: Chain; contract_address: string; block_number: number };
      Querystring: { block_datetime?: string; block_timestamp?: number };
    };

    instance.get<TRoute>("/product/:chain/:contract_address/balances_at/:block_number", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, contract_address, block_number } = req.params;
      const { block_datetime: block_datetime_str, block_timestamp } = req.query;

      if (block_datetime_str && block_timestamp) {
        return reply.code(400).send({ error: "Only provide either block_datetime or block_timestamp" });
      }

      const block_datetime = block_datetime_str ? new Date(block_datetime_str) : block_timestamp ? new Date(block_timestamp * 1000) : null;
      const product = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);

      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }

      const data = await instance.diContainer.cradle.beefyVault.getBalancesAtBlock(product, block_number, block_datetime);
      return reply.send(data);
    });
  }
}
