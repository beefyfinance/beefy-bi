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
export const API_DOMAIN = process.env.API_DOMAIN || "localhost";

export const TIMESCALEDB_URL = process.env.TIMESCALEDB_URL || "psql://beefy:beefy@localhost:5432/beefy";
export const TIMESCALEDB_RO_URL = process.env.TIMESCALEDB_RO_URL || "psql://api_ro:api_ro@localhost:5432/beefy";
export const BATCH_DB_INSERT_SIZE = 5000;
export const BATCH_MAX_WAIT_MS = 5000;

export const MAX_RANGES_PER_PRODUCT_TO_GENERATE = process.env.MAX_RANGES_PER_PRODUCT_TO_GENERATE
  ? parseInt(process.env.MAX_RANGES_PER_PRODUCT_TO_GENERATE, 10)
  : 100;

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const BEEFY_DATA_URL = process.env.BEEFY_DATA_URL || "https://data.beefy.finance";

export const RPC_API_KEY_AURORA = process.env.RPC_API_KEY_AURORA || null;
export const RPC_API_KEY_ANKR = process.env.RPC_API_KEY_ANKR || null;
export const RPC_API_KEY_METIS_OWNER = process.env.RPC_API_KEY_METIS_OWNER || null;
export const RPC_API_KEY_ALCHEMY = process.env.RPC_API_KEY_ALCHEMY || null;
export const RPC_API_KEY_NODEREAL = process.env.RPC_API_KEY_NODEREAL || null;

/*
export const RPC_URLS: { [chain in Chain]: string[] } = {
  arbitrum: process.env.ARBITRUM_RPC
    ? [process.env.ARBITRUM_RPC]
    : [
        // only ankr has a full node
        "https://rpc.ankr.com/arbitrum",
        //"https://arb1.arbitrum.io/rpc"
      ],
  aurora: process.env.AURORA_RPC ? [process.env.AURORA_RPC] : ["https://mainnet.aurora.dev/Fon6fPMs5rCdJc4mxX4kiSK1vsKdzc3D8k6UF8aruek"],
  avax: process.env.AVAX_RPC ? [process.env.AVAX_RPC] : ["https://api.avax.network/ext/bc/C/rpc", "https://rpc.ankr.com/avalanche"],
  bsc: process.env.BSC_RPC
    ? [process.env.BSC_RPC]
    : [
        //https://snapshot-networks.on.fleek.co/56
        "https://bsc-private-dataseed1.nariox.org",
        //"https://bsc-dataseed.binance.org",
        //"https://bsc-dataseed1.defibit.io",
        //"https://bsc-dataseed2.defibit.io",
        //"https://bsc-dataseed1.ninicoin.io",
        //"https://bsc-dataseed3.defibit.io",
        //"https://bsc-dataseed4.defibit.io",
        //"https://bsc-dataseed2.ninicoin.io",
        //"https://bsc-dataseed3.ninicoin.io",
        //"https://bsc-dataseed4.ninicoin.io",
        //"https://bsc-dataseed1.binance.org",
        //"https://bsc-dataseed2.binance.org",
        //"https://bsc-dataseed3.binance.org",
        //"https://bsc-dataseed4.binance.org",
      ],
  celo: process.env.CELO_RPC
    ? [process.env.CELO_RPC]
    : [
        "https://celo.snapshot.org",
        //"https://rpc.ankr.com/celo"
      ],
  cronos: process.env.CRONOS_RPC
    ? [process.env.CRONOS_RPC]
    : [
        "https://evm-cronos.crypto.org",
        //"https://rpc.vvs.finance",
        //"https://evm.cronos.org",
      ],
  emerald: process.env.EMERALD_RPC ? [process.env.EMERALD_RPC] : ["https://emerald.oasis.dev"],
  ethereum: process.env.ETHEREUM_RPC ? [process.env.ETHEREUM_RPC] : ["https://rpc.ankr.com/eth"],
  fantom: process.env.FANTOM_RPC ? [process.env.FANTOM_RPC] : ["https://rpc.ftm.tools", "https://rpcapi.fantom.network"],
  fuse: process.env.FUSE_RPC
    ? [process.env.FUSE_RPC]
    : [
        "https://explorer-node.fuse.io/",
        //"https://rpc.fuse.io",
      ],
  harmony: process.env.HARMONY_RPC
    ? [process.env.HARMONY_RPC]
    : [
        "https://rpc.ankr.com/harmony/",
        //"https://api.s0.t.hmny.io",
      ],
  heco: process.env.HECO_RPC
    ? [process.env.HECO_RPC]
    : [
        "https://http-mainnet.hecochain.com",
        //"https://http-mainnet-node.huobichain.com",
      ],
  kava: process.env.KAVA_RPC ? [process.env.KAVA_RPC] : ["https://evm.kava.io"],
  metis: process.env.METIS_RPC ? [process.env.METIS_RPC] : ["https://andromeda.metis.io/?owner=1088"],
  moonbeam: process.env.MOONBEAM_RPC ? [process.env.MOONBEAM_RPC] : ["https://rpc.api.moonbeam.network"],
  moonriver: process.env.MOONRIVER_RPC
    ? [process.env.MOONRIVER_RPC]
    : [
        //"https://moonriver.api.onfinality.io/public",
        "https://rpc.api.moonriver.moonbeam.network/",
      ],
  optimism: process.env.OPTIMISM_RPC ? [process.env.OPTIMISM_RPC] : ["https://opt-mainnet.g.alchemy.com/v2/JzmIL4Q3jBj7it2duxLFeuCa9Wobmm7D"],
  polygon: process.env.POLYGON_RPC ? [process.env.POLYGON_RPC] : ["https://polygon-rpc.com/"],
  syscoin: process.env.SYSCOIN_RPC ? [process.env.SYSCOIN_RPC] : ["https://rpc.syscoin.org/"],
};
*/
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
  ethereum: 10 * 60 * 1000, // 10 minutes
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
