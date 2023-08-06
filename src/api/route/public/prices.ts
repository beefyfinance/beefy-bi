import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { PriceType, priceTypeSchema } from "../../schema/price-type";
import { productKeySchema } from "../../schema/product";
import { TimeBucket, timeBucketSchema } from "../../schema/time-bucket";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  const schema = {
    querystring: S.object()
      .prop("product_key", productKeySchema.required())
      .prop("price_type", priceTypeSchema.required())
      .prop("time_bucket", timeBucketSchema.required()),

    tags: ["price"],
    summary: "Get a quick price time series for a given product and time bucket",
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
    return reply.send(priceTs.map((price) => [price.datetime, price.price_open, price.price_high, price.price_low, price.price_close]));
  });
}
