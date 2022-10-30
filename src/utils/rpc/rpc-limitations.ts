import * as fs from "fs";
import { cloneDeep } from "lodash";
import { allChainIds, Chain } from "../../types/chain";
import { allRpcCallMethods } from "../../types/rpc-config";
import { CONFIG_DIRECTORY, MIN_DELAY_BETWEEN_RPC_CALLS_MS } from "../config";
import { rootLogger } from "../logger";

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

// make sure we don't hit limitations exactly, apply % margin to be safe
const safetyMargin = {
  eth_getLogs: 0.7,
  eth_call: 0.8,
  eth_getBlockByNumber: 0.7,
  eth_blockNumber: 0.7,
  eth_getTransactionReceipt: 0.5, // this returns a lot of data so make sure we are way below the actual limit
};
export const MAX_RPC_BATCH_SIZE = 500;
export const MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS = 30;

export const defaultLimitations: RpcLimitations = {
  isArchiveNode: false,
  minDelayBetweenCalls: 1000,
  methods: {
    eth_getLogs: null,
    eth_call: null,
    eth_getBlockByNumber: null,
    eth_blockNumber: null,
    eth_getTransactionReceipt: null,
  },
};

const findings = (() => {
  const rawLimitations: { [chain in Chain]: { [rpcUrl: string]: RpcLimitations } } = JSON.parse(
    fs.readFileSync(CONFIG_DIRECTORY + "/rpc-limitations.json", "utf8"),
  );
  // add missing chains
  for (const chain of allChainIds) {
    if (!rawLimitations[chain]) {
      rawLimitations[chain] = {};
    }
  }

  for (const chain of allChainIds) {
    for (const rpcUrl of Object.keys(rawLimitations[chain])) {
      const rpcLimitations = rawLimitations[chain][rpcUrl];
      let wasUpdated = false;
      const limitationCopy = cloneDeep(rpcLimitations);

      for (const method of allRpcCallMethods) {
        const oldLimit = rpcLimitations.methods[method];
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
        if (newLimit !== null && newLimit !== MAX_RPC_BATCH_SIZE) {
          newLimit = Math.floor(newLimit * safetyMargin[method]);
          logger.trace({ msg: "Applying safety margin", data: { chain, rpcUrl, method, oldLimit, newLimit } });
        }

        // disable batching if it's only 1
        if (newLimit !== null && newLimit <= 1) {
          newLimit = null;
          logger.trace({ msg: "Limit is too low, disabling batching", data: { chain, rpcUrl, method, oldLimit, newLimit } });
        }

        if (newLimit !== oldLimit) {
          logger.trace({ msg: "lowering rpc limitation", data: { chain, rpcUrl, method, oldLimit, newLimit } });
          rpcLimitations.methods[method] = newLimit;
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
  methods: {
    eth_getLogs: number | null;
    eth_call: number | null;
    eth_getBlockByNumber: number | null;
    eth_blockNumber: number | null;
    eth_getTransactionReceipt: number | null;
  };
  minDelayBetweenCalls: number | "no-limit";
  isArchiveNode: boolean;
}

export function getRpcLimitations(chain: Chain, rpcUrl: string): RpcLimitations {
  for (const [url, content] of Object.entries(findings[chain])) {
    if (rpcUrl.startsWith(url)) {
      // use the config for the min delay between calls
      content.minDelayBetweenCalls = MIN_DELAY_BETWEEN_RPC_CALLS_MS[chain];
      return content;
    }
  }
  logger.error({ msg: "No rpc limitations found for chain/rpcUrl", data: { chain, rpcUrl } });
  return cloneDeep(defaultLimitations);
}
