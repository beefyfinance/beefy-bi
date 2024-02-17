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
      querystring: S.object().prop(
        "block_datetime",
        S.string().format("date-time").description("The block datetime, can be provided to speed up this endpoint."),
      ),
      tags: ["beefy"],
      summary: "Fetch all investor balances for a specific product at a specific block",
      description: "This endpoint returns all investor balances for a specific product at a specific block",
      response: {
        200: BeefyVaultService.investorBalancesSchema,
      },
    };
    type TRoute = {
      Params: { chain: Chain; contract_address: string; block_number: number };
      Querystring: { block_datetime?: string };
    };

    instance.get<TRoute>("/product/:chain/:contract_address/balances_at/:block_number", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, contract_address, block_number } = req.params;
      const { block_datetime: block_datetime_str } = req.query;

      const block_datetime = block_datetime_str ? new Date(block_datetime_str) : null;
      const product = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);

      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }

      const data = await instance.diContainer.cradle.beefyVault.getBalancesAtBlock(product, block_number, block_datetime);
      return reply.send(data);
    });
  }
}
