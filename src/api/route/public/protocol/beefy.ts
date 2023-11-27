import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { Chain } from "../../../../types/chain";
import { addressSchema } from "../../../schema/address";
import { dateTimeSchema } from "../../../schema/datetime";
import { chainSchema } from "../../../schema/enums";
import { ProductService } from "../../../service/product";
import { BeefyPortfolioService } from "../../../service/protocol/beefy";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  {
    const schema = {
      querystring: S.object().prop("address", addressSchema.required().description("The investor address")),
      tags: ["portfolio"],
      summary: "Fetches the investor timeline",
      description: "Lists all transactions for a given investor, including deposits, withdraws and transfers of share tokens.",
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
      tags: ["stats"],
      summary: "Get investor counts",
      description: "Returns the number of investors for each chain",
      response: {
        200: BeefyPortfolioService.investorCountsSchema,
      },
    };
    type TRoute = {};

    instance.get<TRoute>("/investor-counts", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const investorCounts = await instance.diContainer.cradle.beefy.getInvestorCounts();
      return reply.send(investorCounts);
    });
  }

  {
    const schema = {
      params: S.object().prop("chain", chainSchema.required().description("Only include produce for this chain")),
      querystring: S.object().prop(
        "include_eol",
        S.boolean().default(false).description("Include EOL (end of life) products, you probably never want to set this to true"),
      ),

      tags: ["product"],
      summary: "Fetches all product configurations for a chain",
      description: "Returns the configuration accounted for in databarn",
      response: {
        200: ProductService.allProductsSchema,
      },
    };
    type TRoute = { Params: { chain: Chain }; Querystring: { include_eol: boolean } };

    instance.get<TRoute>("/product/:chain", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain } = req.params;
      const { include_eol } = req.query;
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
      summary: "Fetch a specific product configuration",
      description: "Find one product configuration by chain and contract address",
      response: {
        200: ProductService.oneProductSchema,
      },
    };
    type TRoute = { Params: { chain: Chain; contract_address: string } };

    instance.get<TRoute>("/product/:chain/:contract_address", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, contract_address } = req.params;
      const product = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);

      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }

      return reply.send(product);
    });
  }

  {
    const schema = {
      params: S.object()
        .prop("chain", chainSchema.required().description("Only include products for this chain"))
        .prop("contract_address", addressSchema.required().description("The product contract address")),

      querystring: S.object()
        .prop("from_date_utc", dateTimeSchema.required().description("Inclusive date time to fetch data from, interpreted as utc tz"))
        .prop("to_date_utc", dateTimeSchema.required().description("Exclusive date time to fetch data to, interpreted as utc tz")),

      tags: ["product"],
      summary: "Get tvl snapshots in usd for a specific product.",
      description:
        "This endpoint returns snapshots of the api /tvls endpoints done every 15 minutes and can be used to request raw historical data but the time range must not exceede 1 week and the result will be truncated to 1000 elements. `from_date` is inclusive and `to_date` is exclusive to make is easier to use in loops",

      response: {
        200: {
          type: "array",
          description: "The raw tvl time series",
          items: {
            type: "array",
            description: "The first element is the datetime in ISO format, the second element is the product tvl in usd",
            items: {
              anyOf: [
                { type: "string", format: "date-time" },
                { type: "number", minimum: 0 },
              ],
            },
            example: [["2021-01-01T00:00:00", 1234.34]],
          },
        },
      },
    };
    type TRoute = {
      Params: {
        chain: Chain;
        contract_address: string;
      };
      Querystring: {
        from_date_utc: string;
        to_date_utc: string;
      };
    };

    instance.get<TRoute>("/product/:chain/:contract_address/tvl", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { chain, contract_address } = req.params;
      const { from_date_utc, to_date_utc } = req.query;

      let fromDatetime: Date;
      try {
        fromDatetime = new Date(from_date_utc);
      } catch (e) {
        return reply.code(400).send({ error: "Could not parse `from_date_utc` parameter" });
      }

      let toDatetime: Date;
      try {
        toDatetime = new Date(to_date_utc);
      } catch (e) {
        return reply.code(400).send({ error: "Could not parse `to_date_utc` parameter" });
      }

      if (fromDatetime > toDatetime) {
        return reply.code(400).send({ error: "`from_date_utc` must be before `to_date_utc`" });
      }

      // not more than 3 months
      if (toDatetime.getTime() - fromDatetime.getTime() > 7 * 24 * 60 * 60 * 1000) {
        return reply.code(400).send({ error: "Time range must not exceede 1 week" });
      }

      const product = await instance.diContainer.cradle.product.getProductByChainAndContractAddress(chain, contract_address);
      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }

      if (product.productData.type === "beefy:boost") {
        return reply.code(400).send({ error: "Boost are not supported by this endpoint yet" });
      }

      if (product.productData.vault.eol) {
        return reply.code(400).send({ error: "EOL (end of life) products are not supported by this endpoint yet" });
      }

      const beefyVaultId = product.productData.vault.id;

      const tvls = await instance.diContainer.cradle.beefyApi.getVaultTvl(beefyVaultId, fromDatetime, toDatetime);
      return tvls.map((tvl) => [tvl.datetime.toISOString(), tvl.tvl]);
    });
  }
}
