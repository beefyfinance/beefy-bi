import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { addressSchema } from "../schema/address";
import { productKeySchema } from "../schema/product";
import { TimeBucket, timeBucketSchema, timeBucketToSamplingPeriod } from "../schema/time-bucket";
import { getRateLimitOpts } from "../utils/rate-limiter";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const schema = { querystring: S.object().prop("address", addressSchema.required()) };
  type TRoute = { Querystring: { address: string } };
  const routeOptions = { schema, config: { rateLimit: await getRateLimitOpts() } };

  instance.get<TRoute>("/", routeOptions, async (req, reply) => {
    const { address } = req.query;
    const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
    if (investorId === null) {
      return reply.code(404).send({ error: "Investor not found" });
    }
    const productValues = await instance.diContainer.cradle.portfolio.getInvestorPortfolioValue(investorId);
    return reply.send(productValues);
  });

  instance.get<TRoute>("/timeline", routeOptions, async (req, reply) => {
    const { address } = req.query;
    const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
    if (investorId === null) {
      return reply.code(404).send({ error: "Investor not found" });
    }
    const investorTimeline = await instance.diContainer.cradle.portfolio.getInvestorTimeline(investorId);
    return reply.send(investorTimeline);
  });

  const schema2 = {
    querystring: S.object()
      .prop("address", addressSchema.required())
      .prop("product_key", productKeySchema.required())
      .prop("time_bucket", timeBucketSchema.required()),
  };
  type TRoute2 = { Querystring: { address: string; product_key: string; time_bucket: TimeBucket } };
  const routeOptions2 = { schema: schema2, config: { rateLimit: await getRateLimitOpts() } };
  instance.get<TRoute2>("/product-ts", routeOptions2, async (req, reply) => {
    const { address, product_key, time_bucket } = req.query;
    const investorId = await instance.diContainer.cradle.investor.getInvestorId(address);
    if (investorId === null) {
      return reply.code(404).send({ error: "Investor not found" });
    }
    const product = await instance.diContainer.cradle.product.getProductByProductKey(product_key);
    if (!product) {
      return reply.code(404).send({ error: "Product not found" });
    }

    const { bucketSize, timeRange } = timeBucketToSamplingPeriod(time_bucket);

    const investorTimeline = await instance.diContainer.cradle.portfolio.getInvestorProductUsdValueTs(
      investorId,
      product.productId,
      bucketSize,
      timeRange,
    );
    return reply.send(investorTimeline);
  });

  done();
}
