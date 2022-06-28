import dotenv from "dotenv";
import { Chain } from "../types/chain";
import * as path from "path";
dotenv.config();

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const BEEFY_DATA_URL =
  process.env.BEEFY_DATA_URL || "https://data.beefy.finance";

export const RPC_URLS: { [chain in Chain]: string[] } = {
  arbitrum: process.env.ARBITRUM_RPC
    ? [process.env.ARBITRUM_RPC]
    : [
        // only ankr has a full node
        "https://rpc.ankr.com/arbitrum",
        //"https://arb1.arbitrum.io/rpc"
      ],
  aurora: process.env.AURORA_RPC
    ? [process.env.AURORA_RPC]
    : [
        "https://mainnet.aurora.dev/Fon6fPMs5rCdJc4mxX4kiSK1vsKdzc3D8k6UF8aruek",
      ],
  avax: process.env.AVAX_RPC
    ? [process.env.AVAX_RPC]
    : [
        "https://api.avax.network/ext/bc/C/rpc",
        "https://rpc.ankr.com/avalanche",
      ],
  bsc: process.env.BSC_RPC
    ? [process.env.BSC_RPC]
    : [
        //https://snapshot-networks.on.fleek.co/56
        "https://speedy-nodes-nyc.moralis.io/b9aed21e7bb7bdeb35972c9a/bsc/mainnet/archive",
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
    : ["https://rpc.vvs.finance" /*, "https://evm.cronos.org"*/],
  emerald: process.env.EMERALD_RPC
    ? [process.env.EMERALD_RPC]
    : ["https://emerald.oasis.dev"],
  fantom: process.env.FANTOM_RPC
    ? [process.env.FANTOM_RPC]
    : ["https://rpc.ftm.tools", "https://rpcapi.fantom.network"],
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
  metis: process.env.METIS_RPC
    ? [process.env.METIS_RPC]
    : ["https://andromeda.metis.io/?owner=1088"],
  moonbeam: process.env.MOONBEAM_RPC
    ? [process.env.MOONBEAM_RPC]
    : ["https://rpc.api.moonbeam.network"],
  moonriver: process.env.MOONRIVER_RPC
    ? [process.env.MOONRIVER_RPC]
    : [
        "https://moonriver.api.onfinality.io/public",
        //"https://rpc.api.moonriver.moonbeam.network/",
      ],
  optimism: process.env.OPTIMISM_RPC
    ? [process.env.OPTIMISM_RPC]
    : ["https://opt-mainnet.g.alchemy.com/v2/JzmIL4Q3jBj7it2duxLFeuCa9Wobmm7D"],
  polygon: process.env.POLYGON_RPC
    ? [process.env.POLYGON_RPC]
    : ["https://polygon-rpc.com/"],
  syscoin: process.env.SYSCOIN_RPC
    ? [process.env.SYSCOIN_RPC]
    : ["https://rpc.syscoin.org/"],
};

export const EXPLORER_URLS: { [chain in Chain]: string } = {
  arbitrum: "https://api.arbiscan.io/api",
  aurora: "https://api.aurorascan.dev/api", //"https://explorer.mainnet.aurora.dev/",
  avax: "https://api.snowtrace.io//api",
  bsc: "https://api.bscscan.com/api",
  celo: "https://api.celoscan.xyz/api", // "https://explorer.celo.org/",
  cronos: "https://api.cronoscan.com/api",
  emerald: "https://explorer.emerald.oasis.dev/api", //"https://explorer.oasis.dev/",
  fantom: "https://api.ftmscan.com/api",
  fuse: "https://explorer.fuse.io/api",
  harmony: "https://explorer.harmony.one/api",
  heco: "https://api.hecoinfo.com/api",
  metis: "https://andromeda-explorer.metis.io/api", //"https://stardust-explorer.metis.io/api"
  moonbeam: "https://api-moonbeam.moonscan.io/api",
  moonriver: "https://api-moonriver.moonscan.io/api",
  optimism: "https://api-optimistic.etherscan.io/api",
  polygon: "https://api.polygonscan.com/api",
  syscoin: "https://explorer.syscoin.org/api",
};
export const MIN_DELAY_BETWEEN_EXPLORER_CALLS_MS = 6000;
export const MIN_DELAY_BETWEEN_RPC_CALLS_MS = 1000;

export const CHAIN_RPC_MAX_QUERY_BLOCKS: { [chain in Chain]: number } = {
  arbitrum: 3000,
  aurora: 3000,
  avax: 2048, // requested too many blocks from 3052900 to 3055899, maximum is set to 2048
  bsc: 3000,
  celo: 3000,
  cronos: 3000,
  emerald: 3000,
  fantom: 3000,
  fuse: 3000,
  harmony: 1024, // GetLogs query must be smaller than size 1024
  heco: 3000,
  metis: 3000,
  moonbeam: 3000,
  moonriver: 3000,
  optimism: 3000,
  polygon: 3000,
  syscoin: 3000,
};

export const MS_PER_BLOCK_ESTIMATE: { [chain in Chain]: number } = {
  arbitrum: 2200,
  aurora: 1000,
  avax: 3400,
  bsc: 3630,
  celo: 4000,
  cronos: 5840,
  emerald: 10000,
  fantom: 1900,
  fuse: 5000,
  harmony: 3000,
  heco: 3000,
  metis: 6000,
  moonbeam: 3000,
  moonriver: 13000,
  optimism: 1500,
  polygon: 2170,
  syscoin: 100000,
};

export const DATA_DIRECTORY =
  process.env.DATA_DIRECTORY ||
  path.join(__dirname, "..", "..", "data", "indexed-data");

const log_level = process.env.LOG_LEVEL || "info";
if (!["info", "debug", "verbose", "warn", "error"].includes(log_level)) {
  throw new Error(`Invalid log level ${log_level}`);
}

export const LOG_LEVEL:
  | "info"
  | "debug"
  | "verbose"
  | "trace"
  | "warn"
  | "error" = log_level as any as
  | "info"
  | "debug"
  | "verbose"
  | "trace"
  | "warn"
  | "error";

export const CHAINS_WITH_ETHSCAN_BASED_EXPLORERS: Chain[] = [
  "arbitrum",
  "aurora",
  "avax",
  "bsc",
  "celo",
  "cronos",
  "fantom",
  "harmony",
  "heco",
  "metis",
  "moonbeam",
  "moonriver",
  "optimism",
  "polygon",
];
