import Decimal from "decimal.js";
import dotenv from "dotenv";
import * as path from "path";
import { Chain } from "../types/chain";
import { allLogLevels, LogLevels } from "../types/logger";
import { SamplingPeriod } from "../types/sampling";
dotenv.config();

Decimal.set({
  // make sure we have enough precision
  precision: 50,
  // configure the Decimals lib to format without exponents
  toExpNeg: -250,
  toExpPos: 250,
});

const timezone = process.env.TZ;
if (timezone !== "UTC") {
  throw new Error("Please set TZ=UTC in your .env file or command line");
}

export const API_PORT = parseInt(process.env.API_PORT || "8080", 10);
export const API_LISTEN = process.env.API_LISTEN || "127.0.0.1";
export const API_DISABLE_HTTPS = process.env.API_DISABLE_HTTPS === "true";
export const API_URL = process.env.API_URL || "http://localhost:8080";
export const API_FRONTEND_URL = process.env.API_FRONTEND_URL || "http://localhost:3001";

export const TIMESCALEDB_URL = process.env.TIMESCALEDB_URL || "psql://beefy:beefy@localhost:5432/beefy";
export const BATCH_DB_INSERT_SIZE = 5000;
export const BATCH_MAX_WAIT_MS = 5000;

export const DISABLE_RECENT_IMPORT_SKIP_ALREADY_IMPORTED = process.env.DISABLE_RECENT_IMPORT_SKIP_ALREADY_IMPORTED === "true";
export const USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND = process.env.USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND === "true";

// when a product is marked as EOL, wait this long before removing it from the dashboard and stopping rpc import
export const CONSIDER_PRODUCT_DASHBOARD_EOL_AFTER_X_AFTER_EOL: SamplingPeriod = "1month";

// sometimes the programmer error dump is too large and interferes with the log buffers
// this messes up the log output. set to true to disable the dump
export const DISABLE_PROGRAMMER_ERROR_DUMP = process.env.DISABLE_PROGRAMMER_ERROR_DUMP === "true";

// memory management configs
// Since there is no backpressure system in rxjs, we need to limit the number of incoming items
// Fetching investments is a large operation, so we need to limit the number of concurrent requests
// but other operations are small, so we can allow more items to be streamed into the input queue
export const LIMIT_INVESTMENT_QUERIES = process.env.LIMIT_INVESTMENT_QUERIES ? parseInt(process.env.LIMIT_INVESTMENT_QUERIES, 10) : 100;
export const LIMIT_SHARES_QUERIES = process.env.LIMIT_SHARES_QUERIES ? parseInt(process.env.LIMIT_SHARES_QUERIES, 10) : 1000;
export const LIMIT_SNAPSHOT_QUERIES = process.env.LIMIT_SNAPSHOT_QUERIES ? parseInt(process.env.LIMIT_SNAPSHOT_QUERIES, 10) : 1000;
export const LIMIT_PRICE_QUERIES = process.env.LIMIT_PRICE_QUERIES ? parseInt(process.env.LIMIT_PRICE_QUERIES, 10) : 1000;

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const BEEFY_DATA_URL = process.env.BEEFY_DATA_URL || "https://data.beefy.finance";

export const RPC_API_KEY_AURORA = process.env.RPC_API_KEY_AURORA || null;
export const RPC_API_KEY_ANKR = process.env.RPC_API_KEY_ANKR || null;
export const RPC_API_KEY_METIS_OWNER = process.env.RPC_API_KEY_METIS_OWNER || null;
export const RPC_API_KEY_ALCHEMY = process.env.RPC_API_KEY_ALCHEMY || null;
export const RPC_API_KEY_NODEREAL = process.env.RPC_API_KEY_NODEREAL || null;
export const RPC_API_KEY_NODEREAL_2 = process.env.RPC_API_KEY_NODEREAL_2 || null;
export const RPC_API_KEY_FIGMENT = process.env.RPC_API_KEY_FIGMENT || null;
export const RPC_API_KEY_GETBLOCK = process.env.RPC_API_KEY_GETBLOCK || null;
export const RPC_API_KEY_INFURA = process.env.RPC_API_KEY_INFURA || null;
export const RPC_API_KEY_QUIKNODE = process.env.RPC_API_KEY_QUIKNODE || null;

export const EXPLORER_URLS: { [chain in Chain]: string } = {
  arbitrum: "https://api.arbiscan.io/api",
  aurora: "https://api.aurorascan.dev/api",
  avax: "https://api.snowtrace.io/api",
  bsc: "https://api.bscscan.com/api",
  celo: "https://explorer.celo.org/",
  cronos: "https://api.cronoscan.com/api",
  emerald: "https://explorer.emerald.oasis.dev/",
  ethereum: "https://api.etherscan.io/api",
  fantom: "https://api.ftmscan.com/api",
  fuse: "https://explorer.fuse.io/",
  harmony: "https://explorer.harmony.one/",
  heco: "https://api.hecoinfo.com/api",
  kava: "https://explorer.kava.io",
  metis: "https://andromeda-explorer.metis.io/",
  moonbeam: "https://api-moonbeam.moonscan.io/api",
  moonriver: "https://api-moonriver.moonscan.io/api",
  optimism: "https://api-optimistic.etherscan.io/api",
  polygon: "https://api.polygonscan.com/api",
  syscoin: "https://explorer.syscoin.org/api",
};

function _getExplorerApiKey(chain: Chain) {
  const apiKey = process.env[`ETHERSCAN_API_KEY_${chain.toLocaleUpperCase()}`];
  return apiKey || null;
}
export const ETHERSCAN_API_KEY: {
  [chain in Chain]: string | null;
} = {
  arbitrum: _getExplorerApiKey("arbitrum"),
  aurora: _getExplorerApiKey("aurora"),
  avax: _getExplorerApiKey("avax"),
  bsc: _getExplorerApiKey("bsc"),
  celo: _getExplorerApiKey("celo"),
  cronos: _getExplorerApiKey("cronos"),
  emerald: _getExplorerApiKey("emerald"),
  ethereum: _getExplorerApiKey("ethereum"),
  fantom: _getExplorerApiKey("fantom"),
  fuse: _getExplorerApiKey("fuse"),
  harmony: _getExplorerApiKey("harmony"),
  heco: _getExplorerApiKey("heco"),
  kava: _getExplorerApiKey("kava"),
  metis: _getExplorerApiKey("metis"),
  moonbeam: _getExplorerApiKey("moonbeam"),
  moonriver: _getExplorerApiKey("moonriver"),
  optimism: _getExplorerApiKey("optimism"),
  polygon: _getExplorerApiKey("polygon"),
  syscoin: _getExplorerApiKey("syscoin"),
};

export const MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS = 10_000;

export const BEEFY_PRICE_DATA_MAX_QUERY_RANGE_MS = 1000 * 60 * 60 * 24 * 7 * 4 * 3; // 3 * 4 week (~3 months)

export const MS_PER_BLOCK_ESTIMATE: { [chain in Chain]: number } = {
  arbitrum: 2200,
  aurora: 1000,
  avax: 3400,
  bsc: 3630,
  celo: 4000,
  cronos: 5840,
  emerald: 10000,
  ethereum: 10000,
  fantom: 1900,
  fuse: 5000,
  harmony: 3000,
  heco: 3000,
  kava: 6000,
  metis: 6000,
  moonbeam: 3000,
  moonriver: 13000,
  optimism: 1500,
  polygon: 2170,
  syscoin: 100000,
};

export const CONFIG_DIRECTORY = process.env.CONFIG_DIRECTORY || path.join(__dirname, "..", "..", "data", "config");

export const DATA_DIRECTORY = process.env.DATA_DIRECTORY || path.join(__dirname, "..", "..", "data", "indexed-data");

export const GIT_WORK_DIRECTORY = process.env.GIT_WORK_DIRECTORY || path.join(__dirname, "..", "..", "data", "git-work");

export const GITHUB_RO_AUTH_TOKEN: string | null = process.env.GITHUB_RO_AUTH_TOKEN || null;

const log_level = process.env.LOG_LEVEL || "info";
if (!allLogLevels.includes(log_level)) {
  throw new Error(`Invalid log level ${log_level}`);
}

export const LOG_LEVEL: LogLevels = log_level as LogLevels;
