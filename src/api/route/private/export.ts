import { FastifyInstance, FastifyPluginOptions } from "fastify";
import S from "fluent-json-schema";
import { merge } from "lodash";
import { dateTimeSchema } from "../../schema/datetime";
import { EXPORT_MAX_PAGE_SIZE, ExportDatasetName, exportPageSchema, InvalidExportCursorError } from "../../service/export";

const pageQuerystring = S.object()
  .prop("cursor", S.string().minLength(1).maxLength(500))
  .prop("limit", S.integer().minimum(1).maximum(EXPORT_MAX_PAGE_SIZE).default(1000));
const timeWindowQuerystring = pageQuerystring.prop("from", dateTimeSchema.required()).prop("to", dateTimeSchema.required());

type TRoute = { Querystring: { cursor?: string; limit: number; from?: string; to?: string } };

export default async function (instance: FastifyInstance, opts: FastifyPluginOptions) {
  function addRoute(url: string, dataset: ExportDatasetName, summary: string, { timeWindow }: { timeWindow: boolean }) {
    const schema = {
      querystring: timeWindow ? timeWindowQuerystring : pageQuerystring,
      tags: ["export"],
      summary,
      description: `Rows are ordered and paginated: follow \`next_cursor\` until it is null${
        timeWindow ? ", keeping the same `from` and `to`. Rows are selected with `from <= datetime < to`" : ""
      }.`,
      response: { 200: exportPageSchema(dataset) },
    };
    return instance.get<TRoute>(url, merge({}, opts.routeOpts, { schema }), async (req, reply) => {
      const { cursor, limit } = req.query;
      const from = req.query.from ? new Date(req.query.from) : undefined;
      const to = req.query.to ? new Date(req.query.to) : undefined;
      if (from && to && !(from.getTime() < to.getTime())) {
        return reply.code(400).send({ error: "from must be before to" });
      }

      try {
        return reply.send(await instance.diContainer.cradle.dataExport.getPage(dataset, { cursor, limit, from, to }));
      } catch (err) {
        if (err instanceof InvalidExportCursorError) {
          return reply.code(400).send({ error: err.message });
        }
        throw err;
      }
    });
  }

  addRoute("/products", "products", "List products, this is what resolves product_id", { timeWindow: false });
  addRoute("/price-feeds", "priceFeeds", "List price feeds, this is what resolves price_feed_id", { timeWindow: false });
  addRoute("/prices/raw", "rawPrices", "List every imported price", { timeWindow: true });
  addRoute("/prices/1h", "hourlyPrices", "List hourly prices, only the last 6 months are available", { timeWindow: true });
  addRoute("/prices/1d", "dailyPrices", "List daily prices", { timeWindow: true });
  addRoute("/vault-stats", "vaultStats", "List vault metrics", { timeWindow: true });
  addRoute("/investors", "investors", "List investors, this is what resolves investor_id", { timeWindow: false });
  addRoute("/investor-counts", "investorCounts", "List the daily approximate investor counts per vault", { timeWindow: true });
  addRoute("/investor-timeline", "investorTimeline", "List investor balance changes", { timeWindow: true });
  addRoute("/import-states", "importStates", "List import states, this is what tells which ranges were imported", { timeWindow: false });
}
