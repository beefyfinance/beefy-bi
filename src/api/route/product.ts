import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { productKeySchema } from "../schema/product";
import { getRateLimitOpts } from "../utils/rate-limiter";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const schema = { querystring: S.object().prop("product_key", productKeySchema).required() };
  type TRoute = { Querystring: { product_key: string } };
  const routeOptions = { schema, config: { rateLimit: await getRateLimitOpts() } };

  instance.get<TRoute>("/", routeOptions, async (req, reply) => {
    const { product_key } = req.query;
    const product = await instance.diContainer.cradle.product.getProductByProductKey(product_key);
    if (!product) {
      return reply.code(404).send({ error: "Product not found" });
    }
    // remove technical fields
    const { productId, priceFeedId1, priceFeedId2, pendingRewardsPriceFeedId, ...rest } = product;
    return reply.send(product);
  });

  done();
}
