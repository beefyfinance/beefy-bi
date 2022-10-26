import { cloneDeep } from "lodash";
import { allChainIds, Chain } from "../../types/chain";
import { allRpcCallMethods, RpcCallMethod } from "../../types/rpc-config";
import { MIN_DELAY_BETWEEN_RPC_CALLS_MS } from "../config";
import { rootLogger } from "../logger";
import { ProgrammerError } from "../programmer-error";

const logger = rootLogger.child({ module: "common", component: "rpc-config" });

// virtually lower some numbers to account for internal rpc timeout settings
const internalTimeoutMs = {
  "rpc.ankr.com": 10_000,
  "andromeda.metis.io": 5_000,
  "moonriver.api.onfinality.io": 10_000,
  "rpc.api.moonriver.moonbeam.network": 10_000,
};

// some rpc are just too bad to be used with batching
const disableBatchingFor = {
  "moonriver.api.onfinality.io": true,
  "rpc.api.moonriver.moonbeam.network": true,
};

// make sure we don't hit limitations exactly
const safetyMargin = 0.8;
const maxBatchSize = 500;

const findings = (() => {
  const rawLimitations: { [chain in Chain]: { [rpcUrl: string]: { [method in RpcCallMethod]: number | null } } } = {
    arbitrum: {
      "https://rpc.ankr.com/arbitrum": {
        eth_getLogs: 4,
        eth_call: 4,
        eth_getBlockByNumber: 8,
        eth_blockNumber: null,
      },
    },
    aurora: {
      "https://mainnet.aurora.dev": {
        eth_getLogs: null,
        eth_call: 500,
        eth_getBlockByNumber: 64,
        eth_blockNumber: 500,
      },
    },
    avax: {
      "https://rpc.ankr.com/avalanche": {
        eth_getLogs: null,
        eth_call: 500,
        eth_getBlockByNumber: 500,
        eth_blockNumber: null,
      },
    },
    bsc: {
      "https://rpc.ankr.com/bsc": {
        eth_getLogs: 2,
        eth_call: 500,
        eth_getBlockByNumber: 500,
        eth_blockNumber: null,
      },
    },
    celo: {
      "https://rpc.ankr.com/celo": {
        eth_getLogs: 8,
        eth_call: 1,
        eth_getBlockByNumber: null,
        eth_blockNumber: null,
      },
      "https://celo-mainnet--rpc.datahub.figment.io": {
        eth_getLogs: 26,
        eth_call: 192,
        eth_getBlockByNumber: null,
        eth_blockNumber: 500,
      },
    },
    cronos: {
      "https://evm-cronos.crypto.org": {
        eth_getLogs: 2,
        eth_call: 2,
        eth_getBlockByNumber: 2,
        eth_blockNumber: 2,
      },
    },
    emerald: {
      "https://emerald.oasis.dev": {
        eth_getLogs: 500,
        eth_call: 1,
        eth_getBlockByNumber: 500,
        eth_blockNumber: 500,
      },
    },
    fantom: {
      "https://rpc.ankr.com/fantom": {
        eth_getLogs: 8,
        eth_call: 8,
        eth_getBlockByNumber: 64,
        eth_blockNumber: 4,
      },
    },
    fuse: {
      "https://explorer-node.fuse.io": {
        eth_getLogs: null,
        eth_call: 1,
        eth_getBlockByNumber: 500,
        eth_blockNumber: null,
      },
    },
    harmony: {
      "https://rpc.ankr.com/harmony": {
        eth_getLogs: 232,
        eth_call: 500,
        eth_getBlockByNumber: 500,
        eth_blockNumber: null,
      },
    },
    heco: {
      "https://http-mainnet.hecochain.com": {
        eth_getLogs: 8,
        eth_call: 1,
        eth_getBlockByNumber: 500,
        eth_blockNumber: 500,
      },
    },
    kava: {
      "https://evm.kava.io": {
        eth_getLogs: 2,
        eth_call: 8,
        eth_getBlockByNumber: 16,
        eth_blockNumber: 256,
      },
    },
    metis: {
      "https://andromeda.metis.io": {
        eth_getLogs: 2,
        eth_call: 256,
        eth_getBlockByNumber: 500,
        eth_blockNumber: 500,
      },
    },
    moonbeam: {
      "https://rpc.ankr.com/moonbeam": {
        /*eth_getLogs: 1,
        eth_call: 128,
        eth_getBlockByNumber: 2,
        eth_blockNumber: null,*/
        eth_getLogs: null,
        eth_call: null,
        eth_getBlockByNumber: null,
        eth_blockNumber: null,
      },
    },
    moonriver: {
      "https://rpc.api.moonriver.moonbeam.network": {
        eth_getLogs: 16,
        eth_call: 128,
        eth_getBlockByNumber: 500,
        eth_blockNumber: 500,
      },
    },
    optimism: {
      "https://rpc.ankr.com/optimism": {
        eth_getLogs: 32,
        eth_call: 128,
        eth_getBlockByNumber: 500,
        eth_blockNumber: 500,
      },
    },
    polygon: {
      "https://rpc.ankr.com/polygon": {
        eth_getLogs: 12,
        eth_call: 500,
        eth_getBlockByNumber: 500,
        eth_blockNumber: null,
      },
    },
    syscoin: {
      "https://rpc.ankr.com/syscoin": {
        eth_getLogs: 60,
        eth_call: 1,
        eth_getBlockByNumber: 500,
        eth_blockNumber: null,
      },
    },
  };

  for (const chain of allChainIds) {
    for (const rpcUrl of Object.keys(rawLimitations[chain])) {
      const rpcLimitations = rawLimitations[chain][rpcUrl];
      let wasUpdated = false;
      const limitationCopy = cloneDeep(rpcLimitations);

      for (const method of allRpcCallMethods) {
        const oldLimit = rpcLimitations[method];
        if (oldLimit === null) {
          continue;
        }

        let newLimit: number | null = oldLimit;

        // reduce the limit for those RPCs with a timeout
        for (const internalTimeoutRpc of Object.keys(internalTimeoutMs)) {
          const rpcTimeout = internalTimeoutMs[internalTimeoutRpc as keyof typeof internalTimeoutMs];
          if (rpcUrl.includes(internalTimeoutRpc)) {
            if (rpcTimeout <= 10_000) {
              newLimit = Math.min(30, oldLimit);
              logger.trace({ msg: "Reducing limit for RPC with low timeout", data: { chain, rpcUrl, method, oldLimit, newLimit } });
            } else if (rpcTimeout <= 5_000) {
              newLimit = Math.min(10, oldLimit);
              logger.trace({ msg: "Reducing limit for RPC with low timeout", data: { chain, rpcUrl, method, oldLimit, newLimit } });
            }
          }
        }

        // disable batching if required
        for (const disableBatchingRpc of Object.keys(disableBatchingFor)) {
          const isBatchingDisabled = disableBatchingFor[disableBatchingRpc as keyof typeof disableBatchingFor];
          if (isBatchingDisabled && rpcUrl.includes(disableBatchingRpc)) {
            newLimit = null;
            logger.trace({ msg: "Disabling batching for RPC", data: { chain, rpcUrl, method, oldLimit, newLimit } });
          }
        }

        // apply safety margin
        if (newLimit !== null && newLimit !== maxBatchSize) {
          newLimit = Math.floor(newLimit * safetyMargin);
          logger.trace({ msg: "Applying safety margin", data: { chain, rpcUrl, method, oldLimit, newLimit } });
        }

        // disable batching if it's only 1
        if (newLimit !== null && newLimit <= 1) {
          newLimit = null;
          logger.trace({ msg: "Limit is too low, disabling batching", data: { chain, rpcUrl, method, oldLimit, newLimit } });
        }

        if (newLimit !== oldLimit) {
          logger.trace({ msg: "lowering rpc limitation", data: { chain, rpcUrl, method, oldLimit, newLimit } });
          rpcLimitations[method] = newLimit;
          wasUpdated = true;
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

export interface RpcLimitations {
  eth_getLogs: number | null;
  eth_call: number | null;
  eth_getBlockByNumber: number | null;
  eth_blockNumber: number | null;
  minDelayBetweenCalls: number | "no-limit";
}

export function getRpcLimitations(chain: Chain, rpcUrl: string): RpcLimitations {
  for (const [url, content] of Object.entries(findings[chain])) {
    if (rpcUrl.startsWith(url)) {
      return { ...content, minDelayBetweenCalls: MIN_DELAY_BETWEEN_RPC_CALLS_MS[chain] };
    }
  }
  throw new ProgrammerError({ msg: "No rpc limitations found for chain/rpcUrl", data: { chain, rpcUrl } });
}
