import dotenv from "dotenv";
import { Chain } from "../types/chain";
dotenv.config();

export const DB_URL =
  process.env.DB_URL || "postgres://beefy:beefy@localhost:5432/beefy";

export const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

export const RPC_URLS: { [chain in Chain]: string } = {
  bsc: process.env.BSC_RPC || "https://bsc-dataseed.binance.org",
  heco: process.env.HECO_RPC || "https://http-mainnet.hecochain.com",
  avax: process.env.AVAX_RPC || "https://api.avax.network/ext/bc/C/rpc",
  polygon: process.env.POLYGON_RPC || "https://polygon-rpc.com/",
  fantom: process.env.FANTOM_RPC || "https://rpc.ftm.tools",
  one: process.env.ONE_RPC || "https://api.harmony.one/",
  arbitrum: process.env.ARBITRUM_RPC || "https://arb1.arbitrum.io/rpc",
  celo: process.env.CELO_RPC || "https://forno.celo.org",
  moonriver:
    process.env.MOONRIVER_RPC || "https://moonriver.api.onfinality.io/public",
  cronos: process.env.CRONOS_RPC || "https://rpc.vvs.finance",
  aurora:
    process.env.AURORA_RPC ||
    "https://mainnet.aurora.dev/Fon6fPMs5rCdJc4mxX4kiSK1vsKdzc3D8k6UF8aruek",
  fuse: process.env.FUSE_RPC || "https://rpc.fuse.io",
  metis: process.env.METIS_RPC || "https://andromeda.metis.io/?owner=1088",
  moonbeam: process.env.MOONBEAM_RPC || "https://rpc.api.moonbeam.network",
  sys: process.env.SYS_RPC || "https://rpc.syscoin.org/",
  emerald: process.env.EMERALD_RPC || "https://emerald.oasis.dev",
};

export const EXPLORER_URLS: { [chain in Chain]: string } = {
  cronos: "https://api.cronoscan.com/api",
  bsc: "https://api.bscscan.com/api",
  polygon: "https://api.polygonscan.com/api",
  fantom: "https://api.ftmscan.com/api",
  heco: "https://api.hecoinfo.com/api",
  avax: "https://api.snowtrace.io//api",
  moonbeam: "https://api-moonbeam.moonscan.io/api",
  celo: "https://explorer.celo.org/",
  moonriver: "https://api-moonriver.moonscan.io/api",
  arbitrum: "https://api.arbiscan.io/api",
  aurora: "https://explorer.mainnet.aurora.dev/",
  metis: "https://andromeda-explorer.metis.io/",
  one: "https://explorer.harmony.one/",
  fuse: "https://explorer.fuse.io/",
  sys: "https://explorer.syscoin.org/",
  emerald: "https://explorer.oasis.dev/",
};
