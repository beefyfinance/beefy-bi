import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { getInvestmentsImportStateKey, getPriceFeedImportStateKey } from "../../protocol/beefy/utils/import-state";
import { productKeySchema } from "../schema/product";

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions, done: (err?: Error) => void) {
  const importStateType = S.string().enum(["product_investments", "share_to_underlying", "underlying_to_usd"]).required();

  const schema = {
    querystring: S.object().prop("product_key", productKeySchema.required()).prop("import_type", importStateType.required()),
  };
  type TRoute = {
    Querystring: {
      import_type: "product_investments" | "share_to_underlying" | "underlying_to_usd";
      product_key: string;
    };
  };

  instance.get<TRoute>("/", { schema }, async (req, reply) => {
    const { product_key, import_type } = req.query;
    const product = await instance.diContainer.cradle.product.getProductByProductKey(product_key);
    if (!product) {
      return reply.code(404).send({ error: "Product not found" });
    }

    let importStateKey = "";
    if (import_type === "product_investments") {
      importStateKey = getInvestmentsImportStateKey(product);
    } else {
      // everything else is a price feed

      const priceFeedIdss = await instance.diContainer.cradle.product.getPriceFeedIds([product.productId]);
      const priceFeedIds = priceFeedIdss[0];
      if (!priceFeedIds) {
        return reply.code(404).send({ error: "Price feed not found" });
      }

      let priceFeedId = null;
      if (import_type === "share_to_underlying") {
        priceFeedId = priceFeedIds.price_feed_1_id;
      } else if (import_type === "underlying_to_usd") {
        priceFeedId = priceFeedIds.price_feed_2_id;
      } else {
        return reply.code(404).send({ error: "Price feed not found" });
      }
      importStateKey = getPriceFeedImportStateKey({ priceFeedId: priceFeedId });
    }

    const importState = await instance.diContainer.cradle.importState.getImportStateByKey(importStateKey);
    if (!importState) {
      return reply.code(404).send({ error: "Import state not found" });
    }

    // remove technical id fields
    const filteredImportState = Object.entries(importState.importData)
      .filter(([key, _]) => !key.endsWith("Id"))
      .reduce((acc, [key, value]) => Object.assign(acc, { [key]: value }), {});
    return filteredImportState;
  });

  done();
}
