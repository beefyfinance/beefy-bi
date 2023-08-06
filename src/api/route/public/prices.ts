import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { dateTimeSchema } from "../../schema/datetime";
import { PriceType, priceTypeSchema } from "../../schema/price-type";
import { productKeySchema } from "../../schema/product";
import { TimeBucket, timeBucketSchema } from "../../schema/time-bucket";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  {
    const schema = {
      querystring: S.object()
        .prop("product_key", productKeySchema.required())
        .prop("price_type", priceTypeSchema.required())
        .prop("time_bucket", timeBucketSchema.required()),

      tags: ["price"],
      summary: "Get a quick price time series for a given product and time bucket",
      description:
        "This endpoint is essentially used for charting and UIs as it provides a simple interface to get the most widely displayed time series.",

      response: {
        200: {
          type: "array",
          description: "The price time series",
          items: {
            type: "array",
            description:
              "The first element is the datetime in ISO format, the second element is the open price of the time_bucket, the third element is the high price of the time_bucket, the fourth element is the low price of the time_bucket, the fifth element is the close price of the time_bucket",
            items: {
              anyOf: [{ type: "string", format: "date-time" }, { type: "string" }],
            },
            example: [["2021-01-01T00:00:00", "1000.0000012123", "1000.0000012123", "1000.0000012123", "1000.0000012123"]],
          },
        },
      },
    };
    type TRoute = {
      Querystring: {
        price_type: PriceType;
        product_key: string;
        time_bucket: TimeBucket;
      };
    };

    instance.get<TRoute>("/", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { product_key, price_type, time_bucket } = req.query;
      const product = await instance.diContainer.cradle.product.getProductByProductKey(product_key);
      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }
      const priceFeedIdss = await instance.diContainer.cradle.product.getPriceFeedIds([product.productId]);
      const priceFeedIds = priceFeedIdss[0];
      if (!priceFeedIds) {
        return reply.code(404).send({ error: "Price feed not found" });
      }
      let priceFeedId = null;
      if (price_type === "share_to_underlying") {
        priceFeedId = priceFeedIds.price_feed_1_id;
      } else if (price_type === "underlying_to_usd") {
        priceFeedId = priceFeedIds.price_feed_2_id;
      } else {
        return reply.send([]);
      }
      if (!priceFeedId) {
        return reply.code(404).send({ error: "Price feed not found" });
      }

      const priceTs = await instance.diContainer.cradle.price.getPriceTs(priceFeedId, time_bucket);
      return reply.send(
        priceTs.map((price) => [price.datetime.toISOString(), price.price_open, price.price_high, price.price_low, price.price_close]),
      );
    });
  }

  {
    const schema = {
      querystring: S.object()
        .prop("product_key", productKeySchema.required())
        .prop("price_type", priceTypeSchema.required())
        .prop("from_date_utc", dateTimeSchema.required().description("Inclusive date time to fetch data from, interpreted as utc tz"))
        .prop("to_date_utc", dateTimeSchema.required().description("Exclusive date time to fetch data to, interpreted as utc tz")),

      tags: ["price"],
      summary: "Get a raw time series for a given time range.",
      description:
        "This endpoint can be used to request raw historical data but the time range must not exceede a week and the result will be truncated to 1000 elements. `from_date` is inclusive and `to_date` is exclusive to make is easier to use in loops",

      response: {
        200: {
          type: "array",
          description: "The raw price time series",
          items: {
            type: "array",
            description:
              "The first element is the datetime in ISO format, the second element is the chain block number and the third element the raw price data",
            items: {
              anyOf: [{ type: "string", format: "date-time" }, { type: "integer", minimum: 0 }, { type: "string" }],
            },
            example: [["2021-01-01T00:00:00", 1234, "1000.0000012123"]],
          },
        },
      },
    };
    type TRoute = {
      Querystring: {
        price_type: PriceType;
        product_key: string;
        from_date_utc: string;
        to_date_utc: string;
      };
    };

    instance.get<TRoute>("/raw", merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { product_key, price_type, from_date_utc, to_date_utc } = req.query;

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

      // not more than a week
      if (toDatetime.getTime() - fromDatetime.getTime() > 7 * 24 * 60 * 60 * 1000) {
        return reply.code(400).send({ error: "Time range must not exceede a week" });
      }

      const product = await instance.diContainer.cradle.product.getProductByProductKey(product_key);
      if (!product) {
        return reply.code(404).send({ error: "Product not found" });
      }
      const priceFeedIdss = await instance.diContainer.cradle.product.getPriceFeedIds([product.productId]);
      const priceFeedIds = priceFeedIdss[0];
      if (!priceFeedIds) {
        return reply.code(404).send({ error: "Price feed not found" });
      }
      let priceFeedId = null;
      if (price_type === "share_to_underlying") {
        priceFeedId = priceFeedIds.price_feed_1_id;
      } else if (price_type === "underlying_to_usd") {
        priceFeedId = priceFeedIds.price_feed_2_id;
      } else {
        return reply.send([]);
      }
      if (!priceFeedId) {
        return reply.code(404).send({ error: "Price feed not found" });
      }

      const priceTs = await instance.diContainer.cradle.price.getRawPriceTs(priceFeedId, fromDatetime, toDatetime, 1000);
      console.log(priceTs.map((price) => [price.datetime.toISOString(), price.block_number, price.price]));
      return reply.send(priceTs.map((price) => [price.datetime.toISOString(), price.block_number, price.price]));
    });
  }
}
