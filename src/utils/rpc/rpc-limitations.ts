import * as fs from "fs";
import { cloneDeep, merge } from "lodash";
import { allChainIds, Chain } from "../../types/chain";
import { allRpcCallMethods } from "../../types/rpc-config";
import { CONFIG_DIRECTORY, USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND } from "../config";
import { rootLogger } from "../logger";
import { ProgrammerError } from "../programmer-error";
import { addSecretsToRpcUrl, removeSecretsFromRpcUrl } from "./remove-secrets-from-rpc-url";

const logger = rootLogger.child({ module: "common", component: "rpc-config" });

// make sure we don't hit limitations exactly, apply % margin to be safe
const safetyMargin = {
  eth_getLogs: 0.7,
  eth_call: 0.8,
  eth_getBlockByNumber: 0.7,
  eth_blockNumber: 0.7,
  eth_getTransactionReceipt: 0.5, // this returns a lot of data so make sure we are way below the actual limit
};
export const MAX_RPC_BATCHING_SIZE = 500;
export const MAX_RPC_GETLOGS_SPAN = 5_000;
export const MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS = 30;
// when detecting limitations, we consider a call failed if it takes more than this constant
export const RPC_SOFT_TIMEOUT_MS = 15_000;

export const defaultLimitations: RpcLimitations = {
  isArchiveNode: false,
  minDelayBetweenCalls: 1000,
  maxGetLogsBlockSpan: 10,
  internalTimeoutMs: null,
  disableBatching: false,
  disableRpc: false,
  methods: {
    eth_getLogs: null,
    eth_call: null,
    eth_getBlockByNumber: null,
    eth_blockNumber: null,
    eth_getTransactionReceipt: null,
  },
};

const findings = (() => {
  const rawLimitations = readRawLimitations();

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
        if (limitationCopy.internalTimeoutMs) {
          if (limitationCopy.internalTimeoutMs <= 10_000) {
            newLimit = Math.min(30, oldLimit);
            logger.trace({ msg: "Reducing limit for RPC with low timeout", data: { chain, rpcUrl, method, oldLimit, newLimit } });
          } else if (limitationCopy.internalTimeoutMs <= 5_000) {
            newLimit = Math.min(10, oldLimit);
            logger.trace({ msg: "Reducing limit for RPC with low timeout", data: { chain, rpcUrl, method, oldLimit, newLimit } });
          }
        }

        // disable batching if required
        if (limitationCopy.disableBatching) {
          newLimit = null;
          logger.trace({ msg: "Disabling batching for RPC", data: { chain, rpcUrl, method, oldLimit, newLimit } });
        }

        // apply safety margin
        if (newLimit !== null && newLimit !== MAX_RPC_BATCHING_SIZE) {
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
  // if true, the RPC is an archive node
  // we need this information to know if we can retry ArchiveNodeNeeded errors
  isArchiveNode: boolean;
  // the minimum delay between calls to this RPC
  minDelayBetweenCalls: number | "no-limit";
  // the maximum number of blocks that can be queried with eth_getLogs
  maxGetLogsBlockSpan: number;
  // the internal timeout of the RPC, calls that take longer than this will be aborted
  // we use this information to lower the batch size so that we don't hit the timeout
  internalTimeoutMs: number | null;
  // if true, we disable batching for this RPC
  // used for RPCs that are too slow or unreliable to be used with batching
  disableBatching: boolean;
  // sometimes an RPC is so slow or overloaded that we can't even use it
  // if this is true, we disable all calls to this RPC
  disableRpc: boolean;
  // maximum batching allowed for each method
  // null means batching is not allowed
  methods: {
    eth_getLogs: number | null;
    eth_call: number | null;
    eth_getBlockByNumber: number | null;
    eth_blockNumber: number | null;
    eth_getTransactionReceipt: number | null;
  };
}

export function getRpcLimitations(chain: Chain, rpcUrl: string, forceGetLogsBlockSpan?: number | null): RpcLimitations {
  let limitations = cloneDeep(findings[chain][removeSecretsFromRpcUrl(rpcUrl)]);
  if (!limitations) {
    if (USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND) {
      limitations = cloneDeep(defaultLimitations);
    } else {
      throw new ProgrammerError({
        msg: "No rpc limitations found for chain/rpcUrl. Set USE_DEFAULT_LIMITATIONS_IF_NOT_FOUND=true to use default limitations",
        data: { chain, rpcUrl },
      });
    }
  }
  if (forceGetLogsBlockSpan !== undefined && forceGetLogsBlockSpan !== null) {
    limitations.maxGetLogsBlockSpan = forceGetLogsBlockSpan;
  }
  return limitations;
}

export function getAllRpcUrlsForChain(chain: Chain): string[] {
  const chainRpcs = findings[chain];
  if (!chainRpcs) {
    throw new ProgrammerError({ msg: "No rpcs found for chain", data: { chain } });
  }
  return Object.keys(chainRpcs).map(addSecretsToRpcUrl);
}

export function getBestRpcUrlsForChain(chain: Chain, mode: "historical" | "recent"): string[] {
  const chainRpcs = findings[chain];
  if (!chainRpcs) {
    throw new ProgrammerError({ msg: "No rpcs found for chain", data: { chain } });
  }

  let rpcConfigs = Object.entries(chainRpcs).map(([rpcUrl, limitations]) => ({
    rpcUrl,
    limitations,
  }));

  if (rpcConfigs.length === 0) {
    throw new ProgrammerError({ msg: "No rpcs found for chain", data: { chain } });
  }

  // shortcut when there's only one RPC
  if (rpcConfigs.length === 1) {
    return [addSecretsToRpcUrl(rpcConfigs[0].rpcUrl)];
  }

  // remove disabled RPCs
  rpcConfigs = rpcConfigs.filter((rpcConfig) => !rpcConfig.limitations.disableRpc);

  if (mode === "historical") {
    const historicalRpcConfigs = rpcConfigs.filter((rpcConfig) => rpcConfig.limitations.isArchiveNode);
    if (historicalRpcConfigs.length > 0) {
      rpcConfigs = historicalRpcConfigs;
    } else {
      logger.warn({ msg: "No archive nodes RPC found for chain", data: { chain } });
    }
  }

  // order by no-limit nodes first, then by minDelayBetweenCalls, then by the get logs block span
  rpcConfigs.sort((a, b) => {
    if (a.limitations.minDelayBetweenCalls === "no-limit") {
      return -1;
    }
    if (b.limitations.minDelayBetweenCalls === "no-limit") {
      return 1;
    }
    if (a.limitations.minDelayBetweenCalls !== b.limitations.minDelayBetweenCalls) {
      return a.limitations.minDelayBetweenCalls - b.limitations.minDelayBetweenCalls;
    }
    return b.limitations.maxGetLogsBlockSpan - a.limitations.maxGetLogsBlockSpan;
  });

  return rpcConfigs.map((rpcConfig) => addSecretsToRpcUrl(rpcConfig.rpcUrl));
}

export function readRawLimitations(): { [chain in Chain]: { [rpcUrl: string]: RpcLimitations } } {
  return JSON.parse(fs.readFileSync(CONFIG_DIRECTORY + "/rpc-limitations.json", "utf8"));
}
export function updateRawLimitations(limitationDiff: { [chain in Chain]: { [rpcUrl: string]: RpcLimitations } }): void {
  const rawLimitations = readRawLimitations();
  const updatedLimitations = merge(rawLimitations, limitationDiff);
  fs.writeFileSync(CONFIG_DIRECTORY + "/rpc-limitations.json", JSON.stringify(updatedLimitations, null, 2));
}
