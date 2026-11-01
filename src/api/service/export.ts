import pgf from "pg-format";
import { DbClient, db_query } from "../../utils/db";

export const EXPORT_MAX_PAGE_SIZE = 10_000;

type ExportColumnType = "datetime" | "integer" | "double" | "decimal" | "hex" | "string" | "json";
type ExportColumn = { name: string; type: ExportColumnType; nullable?: boolean; sql?: string };
type ExportDataset = {
  table: string;
  columns: ExportColumn[];
  // unique and ordered key used to paginate, all listed in columns
  key: string[];
};

const column = (name: string, type: ExportColumnType): ExportColumn => ({ name, type });
const nullable = (name: string, type: ExportColumnType): ExportColumn => ({ name, type, nullable: true });
const investorCount = (name: string, hllColumn: string): ExportColumn => ({
  name,
  type: "integer",
  nullable: true,
  sql: `distinct_count(t.${hllColumn})::integer`,
});

const aggregatedPriceColumns = [
  column("datetime", "datetime"),
  column("price_feed_id", "integer"),
  column("price_avg", "decimal"),
  column("price_high", "decimal"),
  column("price_low", "decimal"),
  column("price_open", "decimal"),
  column("price_close", "decimal"),
];

/**
 * This is the only definition of what we export: the queries select these columns and the
 * response schemas are generated from them, so the docs can't disagree with the api.
 * Columns are listed so new sensitive ones are not exported by default, investor_data is left out on purpose.
 */
export const exportDatasets = {
  products: {
    table: "product",
    key: ["product_id"],
    columns: [
      column("product_id", "integer"),
      column("product_key", "string"),
      column("chain", "string"),
      nullable("price_feed_1_id", "integer"),
      nullable("price_feed_2_id", "integer"),
      nullable("pending_rewards_price_feed_id", "integer"),
      column("product_data", "json"),
    ],
  },
  priceFeeds: {
    table: "price_feed",
    key: ["price_feed_id"],
    columns: [
      column("price_feed_id", "integer"),
      column("feed_key", "string"),
      column("from_asset_key", "string"),
      column("to_asset_key", "string"),
      column("price_feed_data", "json"),
    ],
  },
  rawPrices: {
    table: "price_ts",
    key: ["datetime", "price_feed_id", "block_number"],
    columns: [column("datetime", "datetime"), column("price_feed_id", "integer"), column("block_number", "integer"), column("price", "decimal")],
  },
  hourlyPrices: { table: "price_ts_cagg_1h", key: ["datetime", "price_feed_id"], columns: aggregatedPriceColumns },
  dailyPrices: { table: "price_ts_cagg_1d", key: ["datetime", "price_feed_id"], columns: aggregatedPriceColumns },
  vaultStats: {
    table: "beefy_vault_stats_ts",
    key: ["datetime", "product_id", "block_number"],
    columns: [
      column("datetime", "datetime"),
      column("product_id", "integer"),
      column("block_number", "integer"),
      nullable("vault_total_supply", "decimal"),
      nullable("share_to_underlying_price", "decimal"),
      nullable("staked_underlying", "decimal"),
      nullable("underlying_total_supply", "decimal"),
      nullable("underlying_capture_percentage", "double"),
    ],
  },
  investorCounts: {
    table: "product_stats_investor_counts_with_segments_1d_ts",
    key: ["datetime", "product_id"],
    columns: [
      column("datetime", "datetime"),
      column("product_id", "integer"),
      investorCount("approx_investor_count", "approx_count_investor_hll_all"),
      investorCount("approx_investor_count_usd_0_10", "approx_count_investor_hll_0_10"),
      investorCount("approx_investor_count_usd_10_100", "approx_count_investor_hll_10_100"),
      investorCount("approx_investor_count_usd_100_1000", "approx_count_investor_hll_100_1000"),
      investorCount("approx_investor_count_usd_1000_10000", "approx_count_investor_hll_1000_10000"),
      investorCount("approx_investor_count_usd_10000_100000", "approx_count_investor_hll_10000_100000"),
      investorCount("approx_investor_count_usd_100000_plus", "approx_count_investor_hll_100000_1000000"),
    ],
  },
  investors: {
    table: "investor",
    key: ["investor_id"],
    columns: [column("investor_id", "integer"), column("address", "hex")],
  },
  investorTimeline: {
    table: "beefy_investor_timeline_cache_ts",
    key: ["datetime", "investor_id", "product_id", "block_number"],
    columns: [
      column("datetime", "datetime"),
      column("investor_id", "integer"),
      column("product_id", "integer"),
      column("block_number", "integer"),
      nullable("transaction_hash", "hex"),
      nullable("balance", "double"),
      nullable("balance_diff", "double"),
      nullable("share_to_underlying_price", "double"),
      nullable("underlying_balance", "double"),
      nullable("underlying_diff", "double"),
      nullable("underlying_to_usd_price", "double"),
      nullable("usd_balance", "double"),
      nullable("usd_diff", "double"),
      nullable("pending_rewards", "double"),
      nullable("pending_rewards_diff", "double"),
      nullable("pending_rewards_to_usd_price", "double"),
      nullable("pending_rewards_usd_balance", "double"),
      nullable("pending_rewards_usd_diff", "double"),
    ],
  },
} satisfies Record<string, ExportDataset>;

export type ExportDatasetName = keyof typeof exportDatasets;

const columnTypeSchemas: { [key in ExportColumnType]: object } = {
  datetime: { type: "string", description: "UTC ISO 8601 date with microsecond precision" },
  integer: { type: "integer" },
  double: { type: "number", description: "Approximate value" },
  decimal: {
    type: "string",
    pattern: "^-?[0-9]+(\\.[0-9]+)?$",
    description: "Exact decimal, parse it as a decimal and not as a float to keep the precision",
  },
  hex: { type: "string", pattern: "^0x[0-9a-f]*$", description: "Lowercase hex" },
  string: { type: "string" },
  json: { type: "object", additionalProperties: true },
};

export function exportPageSchema(dataset: ExportDatasetName) {
  const { columns } = exportDatasets[dataset];
  return {
    type: "object",
    required: ["data", "next_cursor"],
    properties: {
      data: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          required: columns.map((col) => col.name),
          properties: Object.fromEntries(columns.map((col) => [col.name, { ...columnTypeSchemas[col.type], nullable: !!col.nullable }])),
        },
      },
      next_cursor: { type: "string", nullable: true, description: "Pass it as `cursor` to get the next page, null when this is the last page" },
    },
  };
}

function selectColumn({ name, type, sql }: ExportColumn) {
  if (type === "datetime") {
    // keep the microseconds, a js Date would drop them and the pagination cursor needs them
    return pgf(`to_char(t.%I AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as %I`, name, name);
  }
  if (type === "hex") {
    return pgf(`'0x' || encode(t.%I::bytea, 'hex') as %I`, name, name);
  }
  return sql ? pgf(`${sql} as %I`, name) : pgf("t.%I", name);
}

const CURSOR_DATETIME_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;

function encodeCursor(values: (string | number)[]) {
  return Buffer.from(JSON.stringify(values)).toString("base64url");
}

function decodeCursor(cursor: string, keyColumns: ExportColumn[]): (string | number)[] | null {
  let values: unknown;
  try {
    values = JSON.parse(Buffer.from(cursor, "base64url").toString());
  } catch (err) {
    return null;
  }
  if (!Array.isArray(values) || values.length !== keyColumns.length) {
    return null;
  }
  const valid = keyColumns.every((col, i) =>
    col.type === "datetime" ? typeof values[i] === "string" && CURSOR_DATETIME_REGEX.test(values[i]) : Number.isSafeInteger(values[i]),
  );
  return valid ? values : null;
}

export class InvalidExportCursorError extends Error {}

export class ExportService {
  constructor(private services: { db: DbClient }) {}

  async getPage(datasetName: ExportDatasetName, options: { cursor?: string; limit: number; from?: Date; to?: Date }) {
    const dataset: ExportDataset = exportDatasets[datasetName];
    const keyColumns = dataset.key.map((name) => dataset.columns.find((col) => col.name === name) as ExportColumn);
    const keySql = dataset.key.map((name) => pgf("t.%I", name)).join(", ");

    const conditions = ["true"];
    const params: (string | number)[] = [];
    if (options.from && options.to) {
      conditions.push("t.datetime >= %L", "t.datetime < %L");
      params.push(options.from.toISOString(), options.to.toISOString());
    }
    if (options.cursor !== undefined) {
      const values = decodeCursor(options.cursor, keyColumns);
      if (!values) {
        throw new InvalidExportCursorError("Invalid cursor");
      }
      const placeholders = keyColumns.map((col) => (col.type === "datetime" ? "%L::timestamptz" : "%L::integer"));
      conditions.push(`(${keySql}) > (${placeholders.join(", ")})`);
      params.push(...values);
    }

    const rows = await db_query<Record<string, any>>(
      `
      SELECT ${dataset.columns.map(selectColumn).join(", ")}
      FROM ${pgf.ident(dataset.table)} t
      WHERE ${conditions.join(" AND ")}
      ORDER BY ${keySql}
      LIMIT %L
    `,
      [...params, options.limit],
      this.services.db,
    );

    const lastRow = rows.length === options.limit ? rows[rows.length - 1] : null;
    return { data: rows, next_cursor: lastRow ? encodeCursor(dataset.key.map((name) => lastRow[name])) : null };
  }
}
