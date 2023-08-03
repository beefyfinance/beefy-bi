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
export const API_PRIVATE_TOKENS = process.env.API_PRIVATE_TOKEN?.split(" ") || [Math.random().toFixed(30)];
export const API_FRONTEND_URL = process.env.APP_FRONTEND_URL ? new RegExp(process.env.APP_FRONTEND_URL) : "http://localhost:3001";
export const APP_PR_BUILDS_URL = process.env.APP_PR_BUILDS_URL ? new RegExp(process.env.APP_PR_BUILDS_URL) : "http://localhost:3000";
export const APP_LOCAL_BUILDS_URL = process.env.APP_LOCAL_BUILDS_URL ? new RegExp(process.env.APP_LOCAL_BUILDS_URL) : "http://localhost:3001";

export const TIMESCALEDB_URL = process.env.TIMESCALEDB_URL || "psql://beefy:beefy@localhost:5432/beefy";

// sometimes the programmer error dump is too large and interferes with the log buffers
// this messes up the log output. set to true to disable the dump
export const DISABLE_PROGRAMMER_ERROR_DUMP = process.env.DISABLE_PROGRAMMER_ERROR_DUMP === "true";

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const BEEFY_DATA_URL = process.env.BEEFY_DATA_URL || "https://data.beefy.finance";
export const BEEFY_DATA_KEY = process.env.BEEFY_DATA_KEY || null;

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
export const RPC_API_KEY_LLAMARPC = process.env.RPC_API_KEY_LLAMARPC || null;
export const RPC_API_URL_CHAINSTACK_CRONOS = process.env.RPC_API_URL_CHAINSTACK_CRONOS || null;
export const RPC_API_URL_KAVA_BEEFY = process.env.RPC_API_URL_KAVA_BEEFY || null;
export const RPC_API_URL_FUSE_BEEFY = process.env.RPC_API_URL_FUSE_BEEFY || null;

export const EXPLORER_URLS: { [chain in Chain]: { type: "etherscan" | "blockscout" | "blockscout-json" | "harmony" | "zksync"; url: string } } = {
  arbitrum: { type: "etherscan", url: "https://api.arbiscan.io/api" },
  aurora: { type: "etherscan", url: "https://api.aurorascan.dev/api" },
  avax: { type: "etherscan", url: "https://api.snowtrace.io/api" },
  base: { type: "etherscan", url: "https://api.basescan.org/api" },
  bsc: { type: "etherscan", url: "https://api.bscscan.com/api" },
  canto: { type: "blockscout-json", url: "https://tuber.build" },
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
  zkevm: { type: "etherscan", url: "https://api-zkevm.polygonscan.com/api" },
  zksync: { type: "zksync", url: "https://block-explorer-api.mainnet.zksync.io" },
};

export const MULTICALL3_ADDRESS_MAP: { [chain in Chain]: { multicallAddress: string; createdAtBlock: number } | null } = {
  arbitrum: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 7654707 },
  aurora: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 62907816 },
  avax: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 11907934 },
  base: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 5022 },
  bsc: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 15921452 },
  canto: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 2905789 },
  celo: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 13112599 },
  cronos: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 1963112 },
  emerald: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 1481392 },
  ethereum: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 14353601 },
  fantom: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 33001987 },
  fuse: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 16146628 },
  harmony: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 24185753 },
  heco: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 14413501 },
  kava: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 3661165 },
  metis: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 2338552 },
  moonbeam: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 609002 },
  moonriver: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 1597904 },
  optimism: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 4286263 },
  polygon: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 25770160 },
  zkevm: { multicallAddress: "0xcA11bde05977b3631167028862bE2a173976CA11", createdAtBlock: 57746 },
  zksync: { multicallAddress: "0x47898B2C52C957663aE9AB46922dCec150a2272c", createdAtBlock: 1536804 },
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
  base: _getExplorerApiKey("base"),
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
  zkevm: _getExplorerApiKey("zkevm"),
  zksync: _getExplorerApiKey("zksync"),
};

/**
 * Use the following command:
 * npx ts-node ./src/script/show-estimated-ms-per-block.ts -c <chain>
 */
export const MS_PER_BLOCK_ESTIMATE: { [chain in Chain]: number } = {
  arbitrum: 250,
  aurora: 1000,
  avax: 2000,
  base: 2000,
  bsc: 3000,
  canto: 6000,
  celo: 5000,
  cronos: 5840,
  emerald: 6640,
  ethereum: 12000,
  fantom: 2200,
  fuse: 5000,
  harmony: 2000,
  heco: 3000,
  kava: 6200,
  metis: 4000,
  moonbeam: 12200,
  moonriver: 13000,
  optimism: 375,
  polygon: 2170,
  zkevm: 1208,
  zksync: 1000,
};

export const CONFIG_DIRECTORY = process.env.CONFIG_DIRECTORY || path.join(__dirname, "..", "..", "data", "config");

export const GIT_WORK_DIRECTORY = process.env.GIT_WORK_DIRECTORY || path.join(__dirname, "..", "..", "data", "git-work");

export const GITHUB_RO_AUTH_TOKEN: string | null = process.env.GITHUB_RO_AUTH_TOKEN || null;

const log_level = process.env.LOG_LEVEL || "info";
if (!allLogLevels.includes(log_level)) {
  throw new Error(`Invalid log level ${log_level}`);
}

export const LOG_LEVEL: LogLevels = log_level as LogLevels;
