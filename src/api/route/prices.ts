import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { SamplingPeriod } from "../../types/sampling";
import { productKeySchema } from "../schema/product";
import { getRateLimitOpts } from "../utils/rate-limiter";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const priceType = S.string().enum(["share_to_underlying", "underlying_to_usd"]).required();
  const timeBucketType = S.string().enum(["15min", "1hour", "4hour", "1day", "1week"]).required();
  const schema = {
    querystring: S.object()
      .prop("product_key", productKeySchema.required())
      .prop("price_type", priceType.required())
      .prop("time_bucket", timeBucketType.required()),
  };
  type TRoute = { Querystring: { price_type: "share_to_underlying" | "underlying_to_usd"; product_key: string; time_bucket: SamplingPeriod } };
  const routeOptions = { schema, config: { rateLimit: await getRateLimitOpts() } };

  instance.get<TRoute>("/", routeOptions, async (req, reply) => {
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

    const priceFeedId = price_type === "share_to_underlying" ? priceFeedIds.price_feed_1_id : priceFeedIds.price_feed_2_id;
    const priceTs = await instance.diContainer.cradle.price.getPriceTs(priceFeedId, time_bucket);
    return reply.send(priceTs.map((price) => [price.datetime, price.price]));
  });

  instance.get<TRoute>("/ohlc", routeOptions, async (req, reply) => {
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

    const priceFeedId = price_type === "share_to_underlying" ? priceFeedIds.price_feed_1_id : priceFeedIds.price_feed_2_id;
    const priceTs = await instance.diContainer.cradle.price.getPriceOhlcTs(priceFeedId, time_bucket);
    return reply.send(priceTs.map((price) => [price.datetime, price.open, price.high, price.low, price.close]));
  });

  done();
}
