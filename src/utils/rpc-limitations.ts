import { Chain } from "../types/chain";
import { RpcCallMethod } from "../types/rpc-config";
import { ProgrammerError } from "./rxjs/utils/programmer-error";

const findings: { [chain in Chain]: { [rpcUrl: string]: { [method in RpcCallMethod]: number | null } } } = {
  arbitrum: {
    "https://rpc.ankr.com/arbitrum/": {
      eth_getLogs: 4,
      eth_call: 4,
      eth_getBlockByNumber: null,
      eth_blockNumber: 17,
    },
  },
  aurora: {
    "https://mainnet.aurora.dev/": {
      eth_getLogs: null,
      eth_call: 500,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  avax: {
    "https://rpc.ankr.com/avalanche/": {
      eth_getLogs: null,
      eth_call: 500,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  bsc: {
    "https://rpc.ankr.com/bsc/": {
      eth_getLogs: 500,
      eth_call: 500,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  celo: {
    "https://rpc.ankr.com/celo/": {
      eth_getLogs: 500,
      eth_call: 1,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  cronos: {
    "https://evm-cronos.crypto.org": {
      eth_getLogs: 3,
      eth_call: 3,
      eth_getBlockByNumber: null,
      eth_blockNumber: 1,
    },
  },
  emerald: {
    "https://emerald.oasis.dev": {
      eth_getLogs: null,
      eth_call: 427,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  fantom: {
    "https://rpc.ankr.com/fantom/": {
      eth_getLogs: 500,
      eth_call: 500,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  fuse: {
    "https://explorer-node.fuse.io/": {
      eth_getLogs: null,
      eth_call: null,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  harmony: {
    "https://rpc.ankr.com/harmony/": {
      eth_getLogs: 500,
      eth_call: 500,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  heco: {
    "https://http-mainnet.hecochain.com": {
      eth_getLogs: 500,
      eth_call: 1,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  metis: {
    "https://andromeda.metis.io/?owner=": {
      eth_getLogs: 500,
      eth_call: 499,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  moonbeam: {
    "https://rpc.ankr.com/moonbeam/": {
      eth_getLogs: 500,
      eth_call: 218,
      eth_getBlockByNumber: null,
      eth_blockNumber: 218,
    },
  },
  moonriver: {
    "https://moonriver.api.onfinality.io/public": {
      eth_getLogs: 500,
      eth_call: 249,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  optimism: {
    "https://rpc.ankr.com/optimism/": {
      eth_getLogs: 500,
      eth_call: 500,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  polygon: {
    "https://rpc.ankr.com/polygon/": {
      eth_getLogs: 500,
      eth_call: 499,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
  syscoin: {
    "https://rpc.ankr.com/syscoin/": {
      eth_getLogs: 57,
      eth_call: 1,
      eth_getBlockByNumber: null,
      eth_blockNumber: 500,
    },
  },
};

export function getRpcLimitations(chain: Chain, rpcUrl: string) {
  for (const [url, content] of Object.entries(findings[chain])) {
    if (rpcUrl.startsWith(url)) {
      return content;
    }
  }
  throw new ProgrammerError({ msg: "No rpc limitations found for chain/rpcUrl", data: { chain, rpcUrl } });
}
