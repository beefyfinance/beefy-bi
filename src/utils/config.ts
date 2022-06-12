import dotenv from "dotenv";
import { Chain } from "../types/chain";
import * as path from "path";
dotenv.config();

export const DB_URL =
  process.env.DB_URL || "postgres://beefy:beefy@localhost:5432/beefy";

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const BEEFY_DATA_URL =
  process.env.BEEFY_DATA_URL || "https://data.beefy.finance";

export const RPC_URLS: { [chain in Chain]: string[] } = {
  bsc: process.env.BSC_RPC
    ? [process.env.BSC_RPC]
    : [
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.defibit.io",
        "https://bsc-dataseed2.defibit.io",
        "https://bsc-dataseed1.ninicoin.io",
        "https://bsc-dataseed3.defibit.io",
        "https://bsc-dataseed4.defibit.io",
        "https://bsc-dataseed2.ninicoin.io",
        "https://bsc-dataseed3.ninicoin.io",
        "https://bsc-dataseed4.ninicoin.io",
        "https://bsc-dataseed1.binance.org",
        "https://bsc-dataseed2.binance.org",
        "https://bsc-dataseed3.binance.org",
        "https://bsc-dataseed4.binance.org",
      ],
  heco: process.env.HECO_RPC
    ? [process.env.HECO_RPC]
    : [
        "https://http-mainnet.hecochain.com",
        /*"https://http-mainnet-node.huobichain.com",*/
      ],
  avax: process.env.AVAX_RPC
    ? [process.env.AVAX_RPC]
    : [
        "https://api.avax.network/ext/bc/C/rpc",
        "https://rpc.ankr.com/avalanche",
      ],
  polygon: process.env.POLYGON_RPC
    ? [process.env.POLYGON_RPC]
    : ["https://polygon-rpc.com/"],
  fantom: process.env.FANTOM_RPC
    ? [process.env.FANTOM_RPC]
    : ["https://rpc.ftm.tools"],
  harmony: process.env.HARMONY_RPC
    ? [process.env.HARMONY_RPC]
    : ["https://api.harmony.one/" /*, "https://api.s0.t.hmny.io"*/],
  arbitrum: process.env.ARBITRUM_RPC
    ? [process.env.ARBITRUM_RPC]
    : ["https://arb1.arbitrum.io/rpc"],
  celo: process.env.CELO_RPC
    ? [process.env.CELO_RPC]
    : ["https://forno.celo.org"],
  moonriver: process.env.MOONRIVER_RPC
    ? [process.env.MOONRIVER_RPC]
    : [
        "https://moonriver.api.onfinality.io/public",
        /*"https://rpc.api.moonriver.moonbeam.network/",*/
      ],
  cronos: process.env.CRONOS_RPC
    ? [process.env.CRONOS_RPC]
    : ["https://rpc.vvs.finance" /*, "https://evm.cronos.org"*/],
  aurora: process.env.AURORA_RPC
    ? [process.env.AURORA_RPC]
    : [
        "https://mainnet.aurora.dev/Fon6fPMs5rCdJc4mxX4kiSK1vsKdzc3D8k6UF8aruek",
      ],
  fuse: process.env.FUSE_RPC ? [process.env.FUSE_RPC] : ["https://rpc.fuse.io"],
  metis: process.env.METIS_RPC
    ? [process.env.METIS_RPC]
    : ["https://andromeda.metis.io/?owner=1088"],
  moonbeam: process.env.MOONBEAM_RPC
    ? [process.env.MOONBEAM_RPC]
    : ["https://rpc.api.moonbeam.network"],
  syscoin: process.env.SYSCOIN_RPC
    ? [process.env.SYSCOIN_RPC]
    : ["https://rpc.syscoin.org/"],
  emerald: process.env.EMERALD_RPC
    ? [process.env.EMERALD_RPC]
    : ["https://emerald.oasis.dev"],
};

export const EXPLORER_URLS: { [chain in Chain]: string } = {
  cronos: "https://api.cronoscan.com/api",
  bsc: "https://api.bscscan.com/api",
  polygon: "https://api.polygonscan.com/api",
  fantom: "https://api.ftmscan.com/api",
  heco: "https://api.hecoinfo.com/api",
  avax: "https://api.snowtrace.io//api",
  moonbeam: "https://api-moonbeam.moonscan.io/api",
  celo: "https://api.celoscan.xyz/api", // "https://explorer.celo.org/",
  moonriver: "https://api-moonriver.moonscan.io/api",
  arbitrum: "https://api.arbiscan.io/api",
  aurora: "https://api.aurorascan.dev/api", //"https://explorer.mainnet.aurora.dev/",
  metis: "https://stardust-explorer.metis.io/api", //"https://andromeda-explorer.metis.io/",
  harmony: "https://explorer.harmony.one/api",
  fuse: "https://explorer.fuse.io/api",
  syscoin: "https://explorer.syscoin.org/api",
  emerald: "https://explorer.oasis.dev/",
};

export const CHAIN_RPC_MAX_QUERY_BLOCKS: { [chain in Chain]: number } = {
  cronos: 3000,
  bsc: 3000,
  polygon: 3000,
  fantom: 3000,
  heco: 3000,
  avax: 2048, // requested too many blocks from 3052900 to 3055899, maximum is set to 2048
  moonbeam: 3000,
  celo: 3000,
  moonriver: 3000,
  arbitrum: 3000,
  aurora: 3000,
  metis: 3000,
  harmony: 3000,
  fuse: 3000,
  syscoin: 3000,
  emerald: 3000,
};

export const MS_PER_BLOCK_ESTIMATE: { [chain in Chain]: number } = {
  cronos: 5840,
  bsc: 3630,
  polygon: 2170,
  fantom: 1900,
  heco: 3000,
  avax: 3400,
  moonbeam: 3000,
  celo: 4000,
  moonriver: 13000,
  arbitrum: 2200,
  aurora: 1000,
  metis: 6000,
  harmony: 3000,
  fuse: 5000,
  syscoin: 100000,
  emerald: 10000,
};

export const DATA_DIRECTORY =
  process.env.DATA_DIRECTORY ||
  path.join(__dirname, "..", "..", "data", "indexed");

const log_level = process.env.LOG_LEVEL || "info";
if (!["info", "debug", "verbose", "warn", "error"].includes(log_level)) {
  throw new Error(`Invalid log level ${log_level}`);
}

export const LOG_LEVEL: "info" | "debug" | "verbose" | "warn" | "error" =
  log_level as any as "info" | "debug" | "verbose" | "warn" | "error";
