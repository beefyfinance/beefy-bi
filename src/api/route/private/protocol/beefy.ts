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
        .prop("block_number", S.number().required().description("The block number to fetch the balances at"))
        .prop(
          "block_datetime",
          S.string()
            .format("date-time")
            .required()
            .description(
              "The block datetime, needs to be provided for performance reasons to narrow the search range of the investments and prices.",
            ),
        ),
      tags: ["beefy"],
      summary: "Fetch all investor balances for a specific product at a specific block",
      description: "This endpoint returns all investor balances for a specific product at a specific block",
      response: {
        200: BeefyVaultService.investorBalancesSchema,
      },
    };
    type TRoute = { Params: { chain: Chain; contract_address: string; block_number: number; block_datetime: string } };

    instance.get<TRoute>(
      "/product/:chain/:contract_address/balances_at/:block_number/:block_datetime",
      merge({}, opts.routeOpts, { schema }),
      async (req, reply) => {
        const { chain, contract_address, block_number, block_datetime: block_datetime_str } = req.params;
        const block_datetime = new Date(block_datetime_str);
        const product = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);

        if (!product) {
          return reply.code(404).send({ error: "Product not found" });
        }

        const data = await instance.diContainer.cradle.beefyVault.getBalancesAtBlock(product, block_number, block_datetime);
        return reply.send(data);
      },
    );
  }
}
