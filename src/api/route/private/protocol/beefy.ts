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
      tags: ["beefy"],
      summary: "Fetch all investor balances for a specific product at a specific block",
      description: "This endpoint returns all investor balances for a specific product at a specific block",
      response: {
        200: BeefyVaultService.investorBalancesSchema,
      },
    };
    type TRoute = { Params: { chain: Chain; contract_address: string; block_number: number } };

    instance.get<TRoute>("/product/:chain/:contract_address/balances_at/:block_number", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, contract_address } = req.params;
      const product = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);

      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }

      console.log(product);
      const data = await instance.diContainer.cradle.beefyVault.getBalancesAtBlock(product, req.params.block_number);
      return reply.send(data);
    });
  }
}
