import dotenv from "dotenv";
import { Chain } from "../types/chain";
import * as path from "path";
dotenv.config();

export const TIMESCALEDB_URL =
  process.env.TIMESCALEDB_URL || "psql://beefy:beefy@localhost:5432/beefy";

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

// -1 means no delay and no locking
// 0 means no delay but one call at a time (locking)
// > 0 is is minimum delay in ms between calls
function _getDelayFromEnv(chain: Chain) {
  const delay =
    process.env[`MIN_DELAY_BETWEEN_RPC_CALLS_${chain.toLocaleUpperCase()}_MS`];
  if (delay) {
    const delayMs = parseInt(delay, 10);
    if (delayMs < 0) {
      return "no-limit";
    } else {
      return delayMs;
    }
  }
  return 1000; // default to 1s between calls
}
export const MIN_DELAY_BETWEEN_RPC_CALLS_MS: {
  [chain in Chain]: number | "no-limit";
} = {
  arbitrum: _getDelayFromEnv("arbitrum"),
  aurora: _getDelayFromEnv("aurora"),
  avax: _getDelayFromEnv("avax"),
  bsc: _getDelayFromEnv("bsc"),
  celo: _getDelayFromEnv("celo"),
  cronos: _getDelayFromEnv("cronos"),
  emerald: _getDelayFromEnv("emerald"),
  fantom: _getDelayFromEnv("fantom"),
  fuse: _getDelayFromEnv("fuse"),
  harmony: _getDelayFromEnv("harmony"),
  heco: _getDelayFromEnv("heco"),
  metis: _getDelayFromEnv("metis"),
  moonbeam: _getDelayFromEnv("moonbeam"),
  moonriver: _getDelayFromEnv("moonriver"),
  optimism: _getDelayFromEnv("optimism"),
  polygon: _getDelayFromEnv("polygon"),
  syscoin: _getDelayFromEnv("syscoin"),
};

export const RPC_BACH_CALL_COUNT: {
  [chain in Chain]: number | "no-batching";
} = {
  arbitrum: 100,
  aurora: 100,
  avax: 100,
  bsc: 40, // current RPC don't like too big batches
  celo: 100,
  cronos: "no-batching", // cronos doesn't look it supports batching or ethers don't understand the response
  emerald: "no-batching", // fetching fetch:emerald:0x74d6b1D419556d8A3E3038A9c8096DA0cA4beF24:creation_date: invalid request: max allowed of rounds in logs query is: 100
  fantom: 100,
  fuse: 50, // fuse is a bit slower than the others
  harmony: 50, // harmony is a bit sensitive when batching too much (BUFFER_OVERRUN)
  heco: 100,
  metis: 50, // metis has large blocks
  moonbeam: 100,
  moonriver: 10, // too slow to handle much more
  optimism: 100,
  polygon: 100,
  syscoin: 100,
};

export const CHAIN_RPC_MAX_QUERY_BLOCKS: { [chain in Chain]: number } = {
  arbitrum: 3000,
  aurora: 3000,
  avax: 2048, // requested too many blocks from 3052900 to 3055899, maximum is set to 2048
  bsc: 3000,
  celo: 3000,
  cronos: 1000, // 2k for https://evm-cronos.crypto.org
  emerald: 100, // invalid request: max allowed of rounds in logs query is: 100
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

export const GIT_WORK_DIRECTORY =
  process.env.GIT_WORK_DIRECTORY ||
  path.join(__dirname, "..", "..", "data", "git-work");

export const GITHUB_RO_AUTH_TOKEN: string | null =
  process.env.GITHUB_RO_AUTH_TOKEN || null;

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
  // aurora explorer is missing transfer events
  // ex: this trx is still "indexing" https://aurorascan.dev/tx/0x25baaf56da97b0aff012fcc571476cbb53a6d99c21aed0ae2c7bdb9d217045fe)
  // without this trx, we have a user who'se only trx is a withdrawal: https://aurorascan.dev/token/0x1c9270ac5c42e51611d7b97b1004313d52c80293?a=0x9fd3174fe380591837047ccc0be7ac24b1ea9772
  // Aurora RPC and explorer also disagree on block numbers for the same trx
  // Explorer: https://aurorascan.dev/tx/0xbaba4c1f10dd6773a895a30a07e4ce7829145fb371facbab3c95d6415630e71e
  //  -> block number: 64055210
  // RPC: curl -s "https://mainnet.aurora.dev" -X POST -H "Content-Type: application/json" --data '{"method":"eth_getLogs","params":[{"address": "0x1c9270ac5C42E51611d7b97b1004313D52c80293","fromBlock": "0x3D167A9", "toBlock": "0x3D167AB"}],"id":1,"jsonrpc":"2.0"}' | jq
  //  -> block number: 0x3d167ab -> 64055211
  //"aurora",
  "avax",
  "bsc",
  "celo",
  // cronos explorer has missing events, like aurora https://cronoscan.com/token/0x2425d707a5C63ff5De83eB78f63e06c3f6eEaA1c?a=0x129Dd111C23a8AE4a14694eeb5fAAd7cE9Ed19e1
  //"cronos",
  "fantom",
  "harmony",
  "heco",
  //"metis", too buggy to work with @see below for details
  "moonbeam",
  "moonriver",
  "optimism",
  "polygon",
];

/**
 * Why we don't use metis explorer api:
 *
 * https://andromeda-explorer.metis.io/api-docs
 * Error calling explorer https://andromeda-explorer.metis.io/api: {"message":"Required query parameters missing: topic0_1_opr","result":null,"status":"0"}
 * 
 * So we need to provide an extra parameter for metis, 2 values are possible: "or" and "and"
 * if we use "or" we get too much data
 * if we use "and" don't get any data
 *
 * Example with:
 *  - contract 0x0624ab4290f9305fb2de3fb287aa5cdcf36d6b51
 *  - trx https://andromeda-explorer.metis.io/tx/0xd5a5f77b2f0d012407adcbf10ff34ede63c42ade473d9273812a62444d3ca705/logs
 *
 * RPC and explorer api show logs for this transaction from this contract:
 *   curl "https://andromeda.metis.io/?owner=1088" -X POST -H "Content-Type: application/json" --data '{"method":"eth_getLogs","params":[{"address": "0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000","fromBlock": "0x2D88A8", "toBlock": "0x2D88A8"}],"id":1,"jsonrpc":"2.0"}' | jq
 *   {
      "address": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000000624ab4290f9305fb2de3fb287aa5cdcf36d6b51",
        "0x000000000000000000000000c9b290ff37fa53272e9d71a0b13a444010af4497"
      ],
      "data": "0x000000000000000000000000000000000000000000000000187ab5b9e1c339c3",
      "blockNumber": "0x2d88a8",
      "transactionHash": "0xd5a5f77b2f0d012407adcbf10ff34ede63c42ade473d9273812a62444d3ca705",
      "transactionIndex": "0x0",
      "blockHash": "0xeb38b4b0fd9525f8eb17c7fc782f615829594bb85c3ec81c9dd2e8ab199146e7",
      "logIndex": "0x15",
      "removed": false
    }

 * But we don't get any data from explorer api
 *   https://andromeda-explorer.metis.io/api?module=logs&action=getLogs&address=0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef&fromBlock=2984004&toBlock=2984204&topic1=0x0000000000000000000000000624ab4290f9305fb2de3fb287aa5cdcf36d6b51&topic0_1_opr=and
 *  {"message":"No logs found","result":[],"status":"0"}
 */
/*
interface ChainConfig {
  name: Chain;
  rpcEndpoints: {
    url: string;
    rateLimit: {
      sharedLockProtected: boolean;
      minDelayBetweenCallsMs: number;
    };
    batch: {
      erc20Logs: number;
      ppfsCalls: number;
    };
    maxBlocksPerQuery: number;
  }[];
  ethscanApiExplorer: {
    url: string;
    rateLimit: {
      sharedLockProtected: boolean;
      minDelayBetweenCallsMs: number;
    };
  };
  msPerBlockEstimate: number;
}

const CHAIN_CONFIGS: Record<Chain, ChainConfig> = {};
*/
