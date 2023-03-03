import Decimal from "decimal.js";
import dotenv from "dotenv";
import * as path from "path";
import { Chain } from "../types/chain";
import { allLogLevels, LogLevels } from "../types/logger";
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
export const APP_PR_BUILDS_URL = process.env.APP_PR_BUILDS_URL ? new RegExp(process.env.APP_PR_BUILDS_URL) : "http://localhost:3000";
export const APP_LOCAL_BUILDS_URL = process.env.APP_LOCAL_BUILDS_URL || "http://localhost:3001";

export const TIMESCALEDB_URL = process.env.TIMESCALEDB_URL || "psql://beefy:beefy@localhost:5432/beefy";

// sometimes the programmer error dump is too large and interferes with the log buffers
// this messes up the log output. set to true to disable the dump
export const DISABLE_PROGRAMMER_ERROR_DUMP = process.env.DISABLE_PROGRAMMER_ERROR_DUMP === "true";

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const BEEFY_DATA_URL = process.env.BEEFY_DATA_URL || "https://data.beefy.finance";

export const RPC_API_KEY_AURORA = process.env.RPC_API_KEY_AURORA || null;
export const RPC_API_KEY_ANKR = process.env.RPC_API_KEY_ANKR || null;
export const RPC_API_KEY_METIS_OWNER = process.env.RPC_API_KEY_METIS_OWNER || null;
export const RPC_API_KEY_ALCHEMY_OPTIMISM = process.env.RPC_API_KEY_ALCHEMY_OPTIMISM || null;
export const RPC_API_KEY_ALCHEMY_ARBITRUM = process.env.RPC_API_KEY_ALCHEMY_ARBITRUM || null;
export const RPC_API_KEY_NODEREAL = process.env.RPC_API_KEY_NODEREAL || null;
export const RPC_API_KEY_NODEREAL_2 = process.env.RPC_API_KEY_NODEREAL_2 || null;
export const RPC_API_KEY_FIGMENT = process.env.RPC_API_KEY_FIGMENT || null;
export const RPC_API_KEY_GETBLOCK = process.env.RPC_API_KEY_GETBLOCK || null;
export const RPC_API_KEY_INFURA = process.env.RPC_API_KEY_INFURA || null;
export const RPC_API_KEY_QUIKNODE = process.env.RPC_API_KEY_QUIKNODE || null;
export const RPC_API_URL_CHAINSTACK_CRONOS = process.env.RPC_API_URL_CHAINSTACK_CRONOS || null;

export const EXPLORER_URLS: { [chain in Chain]: { type: "etherscan" | "blockscout" | "blockscout-json" | "harmony"; url: string } } = {
  arbitrum: { type: "etherscan", url: "https://api.arbiscan.io/api" },
  aurora: { type: "etherscan", url: "https://api.aurorascan.dev/api" },
  avax: { type: "etherscan", url: "https://api.snowtrace.io/api" },
  bsc: { type: "etherscan", url: "https://api.bscscan.com/api" },
  canto: { type: "blockscout-json", url: "https://evm.explorer.canto.io" },
  celo: { type: "blockscout", url: "https://explorer.celo.org/" },
  cronos: { type: "etherscan", url: "https://api.cronoscan.com/api" },
  emerald: { type: "blockscout", url: "https://explorer.emerald.oasis.dev/" },
  ethereum: { type: "etherscan", url: "https://api.etherscan.io/api" },
  fantom: { type: "etherscan", url: "https://api.ftmscan.com/api" },
  fuse: { type: "blockscout", url: "https://explorer.fuse.io/" },
  harmony: { type: "harmony", url: "https://explorer.harmony.one/" },
  heco: { type: "etherscan", url: "https://api.hecoinfo.com/api" },
  kava: { type: "blockscout-json", url: "https://explorer.kava.io" },
  metis: { type: "blockscout", url: "https://andromeda-explorer.metis.io/" },
  moonbeam: { type: "etherscan", url: "https://api-moonbeam.moonscan.io/api" },
  moonriver: { type: "etherscan", url: "https://api-moonriver.moonscan.io/api" },
  optimism: { type: "etherscan", url: "https://api-optimistic.etherscan.io/api" },
  polygon: { type: "etherscan", url: "https://api.polygonscan.com/api" },
  syscoin: { type: "etherscan", url: "https://explorer.syscoin.org/api" },
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
  canto: _getExplorerApiKey("canto"),
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

export const MS_PER_BLOCK_ESTIMATE: { [chain in Chain]: number } = {
  arbitrum: 2200,
  aurora: 1000,
  avax: 3400,
  bsc: 3630,
  canto: 6000,
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
