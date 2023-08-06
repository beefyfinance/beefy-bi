import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { Chain } from "../../../../types/chain";
import { addressSchema } from "../../../schema/address";
import { chainSchema } from "../../../schema/enums";
import { ProductService } from "../../../service/product";
import { BeefyPortfolioService } from "../../../service/protocol/beefy";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  {
    const schema = {
      querystring: S.object().prop("address", addressSchema.required()),
      tags: ["portfolio"],
      summary: "Fetches the investor timeline",
      description: "Describes all transactions for a given investor",
      response: {
        200: BeefyPortfolioService.investorTimelineSchema,
      },
    };
    type TRoute = { Querystring: { address: string } };

    instance.get<TRoute>("/timeline", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { address } = req.query;
      const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
      if (investorId === null) {
        return reply.code(404).send({ error: "Investor not found" });
      }
      const investorTimeline = await instance.diContainer.cradle.beefy.getInvestorTimeline(investorId);
      return reply.send(investorTimeline);
    });
  }

  {
    const schema = {
      querystring: S.object()
        .prop("chain", chainSchema.required().description("Only include produce for this chain"))
        .prop(
          "include_eol",
          S.boolean().default(false).description("Include EOL (end of life) products, you probably never want to set this to true"),
        ),

      tags: ["product"],
      summary: "Fetches all products",
      description: "Fetches all product data included in databarn",
      response: {
        200: ProductService.allProductsSchema,
      },
    };
    type TRoute = { Querystring: { include_eol: boolean; chain: Chain } };

    instance.get<TRoute>("/product", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { include_eol, chain } = req.query;
      const allProducts = await instance.diContainer.cradle.product.getAllProducts(include_eol, chain);
      return reply.send(allProducts);
    });
  }

  {
    const schema = {
      params: S.object()
        .prop("chain", chainSchema.required().description("Only include products for this chain"))
        .prop("contract_address", addressSchema.required().description("The product contract address")),

      tags: ["product"],
      summary: "Fetch one product",
      description: "Find one product configuration by chain and contract address",
      response: {
        200: ProductService.oneProductSchema,
      },
    };
    type TRoute = { Params: { chain: Chain; contract_address: string } };

    instance.get<TRoute>("/product/:chain/:contract_address", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, contract_address } = req.params;
      const allProducts = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);
      return reply.send(allProducts);
    });
  }
}
