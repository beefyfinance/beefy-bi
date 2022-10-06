import { cloneDeep } from "lodash";
import { allChainIds, Chain } from "../types/chain";
import { allRpcCallMethods, RpcCallMethod } from "../types/rpc-config";
import { rootLogger } from "./logger";
import { ProgrammerError } from "./rxjs/utils/programmer-error";

const logger = rootLogger.child({ module: "common", component: "rpc-config" });

const findings = (() => {
  const rawLimitations: { [chain in Chain]: { [rpcUrl: string]: { [method in RpcCallMethod]: number | null } } } = {
    arbitrum: {
      "https://rpc.ankr.com/arbitrum/": {
        eth_getLogs: 3,
        eth_call: 3,
        eth_getBlockByNumber: null,
        eth_blockNumber: 3,
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

  // virtually lower some numbers to account for internal rpc timeout settings
  const internalTimeoutMs = {
    "rpc.ankr.com": 10_000,
    "andromeda.metis.io": 5_000,
  };

  for (const chain of allChainIds) {
    for (const rpcUrl of Object.keys(rawLimitations[chain])) {
      const rpcLimitations = rawLimitations[chain][rpcUrl];
      let wasUpdated = false;
      const limitationCopy = cloneDeep(rpcLimitations);

      for (const internalTimeoutRpc of Object.keys(internalTimeoutMs)) {
        const rpcTimeout = internalTimeoutMs[internalTimeoutRpc as keyof typeof internalTimeoutMs];

        if (rpcUrl.includes(internalTimeoutRpc)) {
          for (const method of allRpcCallMethods) {
            const oldLimit = rpcLimitations[method];
            if (oldLimit !== null) {
              let newLimit = oldLimit;

              if (rpcTimeout <= 10_000) {
                newLimit = Math.min(30, oldLimit);
              } else if (rpcTimeout <= 5_000) {
                newLimit = Math.min(10, oldLimit);
              }

              if (newLimit !== oldLimit) {
                logger.trace({ msg: "lowering rpc limitation", data: { chain, rpcUrl, method, oldLimit, newLimit } });
                rpcLimitations[method] = newLimit;
                wasUpdated = true;
              }
            }
          }
        }
      }

      if (wasUpdated) {
        logger.debug({ msg: "updated rpc limitations", data: { chain, rpcUrl, rawLimits: limitationCopy, newLimits: rpcLimitations } });
      } else {
        logger.trace({ msg: "no rpc limitations updated", data: { chain, rpcUrl } });
      }
    }
  }

  return rawLimitations;
})();

export function getRpcLimitations(chain: Chain, rpcUrl: string) {
  for (const [url, content] of Object.entries(findings[chain])) {
    if (rpcUrl.startsWith(url)) {
      return content;
    }
  }
  throw new ProgrammerError({ msg: "No rpc limitations found for chain/rpcUrl", data: { chain, rpcUrl } });
}
