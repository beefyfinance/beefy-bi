import { ethers } from "ethers";
import yargs from "yargs";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { allChainIds, Chain } from "../types/chain";
import { RpcCallMethod } from "../types/rpc-config";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import { sleep } from "../utils/async";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, MIN_DELAY_BETWEEN_RPC_CALLS_MS, RPC_URLS } from "../utils/config";
import { addDebugLogsToProvider, monkeyPatchEthersBatchProvider } from "../utils/ethers";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { ProgrammerError } from "../utils/programmer-error";
import { removeSecretsFromRpcUrl } from "../utils/rpc/remove-secrets-from-rpc-url";

const logger = rootLogger.child({ module: "script", component: "find-out-rpc-limitations" });

const findings = {} as {
  [chain in Chain]: {
    [rpcUrl: string]: {
      [call in RpcCallMethod]: number | null;
    };
  };
};

async function main() {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    rpc: { type: "string", demand: false, alias: "r", describe: "only use this rpc url" },
    chain: { type: "string", demand: false, alias: "c", describe: "only use this chain" },
  }).argv;
  const rpcFilter = argv.rpc;
  const chainFilter = argv.chain as Chain | undefined;

  if (rpcFilter) {
    if (!chainFilter) {
      throw new Error("If you specify an rpc url, you must also specify a chain");
    }
    findings[chainFilter] = {};
    await testRpcLimits(chainFilter, rpcFilter);
  } else {
    const allParams = allChainIds.map((chain) => RPC_URLS[chain].map((rpcUrl) => ({ chain, rpcUrl }))).flat();

    for (const { chain } of allParams) {
      findings[chain] = {};
    }

    await Promise.all(allParams.map(({ chain, rpcUrl }) => testRpcLimits(chain, rpcUrl)));
  }

  logger.info({ msg: "All done", data: { findings } });
}

runMain(main);

/**
 * We want to find out how many batch calls we can make at once for each call type (eth_call, eth_getLogs, etc)
 *
 * With this information, generate a config file we can use on execution
 **/
async function testRpcLimits(chain: Chain, rpcUrl: string) {
  findings[chain][removeSecretsFromRpcUrl(rpcUrl)] = {
    eth_getLogs: null,
    eth_call: null,
    eth_getBlockByNumber: null,
    eth_blockNumber: null,
  };

  logger.info({ msg: "testing rpc", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });

  const rpcOptions: ethers.utils.ConnectionInfo = { url: rpcUrl, timeout: 5_000 /* should respond fairly fast */ };
  const batchProvider = new ethers.providers.JsonRpcBatchProvider(rpcOptions);
  monkeyPatchEthersBatchProvider(batchProvider);

  const linearProvider = new ethers.providers.JsonRpcProvider(rpcOptions);
  addDebugLogsToProvider(batchProvider);
  addDebugLogsToProvider(linearProvider);

  // find out the latest block number
  logger.info({ msg: "fetching latest block number", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  const latestBlockNumber = await linearProvider.getBlockNumber();
  const maxBlocksPerQuery = Math.max(
    1,
    Math.floor(
      /*
        reduce the max number of blocks because we are hitting
        a very heavy traffic contract (wgas contract) 
        some rpc (bsc) have a hard time returning large amounts of data
      */
      CHAIN_RPC_MAX_QUERY_BLOCKS[chain] / 10,
    ),
  );

  const createSaveFinding = (key: RpcCallMethod) => (n: number) => {
    if ((findings[chain][removeSecretsFromRpcUrl(rpcUrl)][key] || 0) < n) {
      findings[chain][removeSecretsFromRpcUrl(rpcUrl)][key] = n;
      logger.debug({ msg: "Updated findings", data: { rpcUrl: removeSecretsFromRpcUrl(rpcUrl), chain, method: key, newValue: n } });
    }
  };

  // eth_getLogs
  logger.info({ msg: "Testing batch eth_getLogs limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(chain, createSaveFinding("eth_getLogs"), (i) => {
    const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20Abi, batchProvider);
    const eventFilter = contract.filters.Transfer();
    const promises = Array.from({ length: i }).map((_, i) => {
      const fromBlock = latestBlockNumber - (i + 1) * maxBlocksPerQuery;
      const toBlock = latestBlockNumber - i * maxBlocksPerQuery;
      return contract.queryFilter(eventFilter, fromBlock, toBlock);
    });
    return Promise.all(promises);
  });

  // eth_call
  logger.info({ msg: "Testing batch eth_call limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(chain, createSaveFinding("eth_call"), (i) => {
    const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20Abi, batchProvider);
    const promises = Array.from({ length: i }).map((_, i) => {
      const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
      return contract.balanceOf(getChainWNativeTokenAddress(chain), { blockTag });
    });
    return Promise.all(promises);
  });

  // eth_getBlockByNumber
  logger.info({ msg: "Testing batch eth_getBlockByNumber limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(chain, createSaveFinding("eth_getBlockByNumber"), (i) => {
    const promises = Array.from({ length: i }).map((_, i) => {
      const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
      return batchProvider.getBlock(blockTag);
    });
    return Promise.all(promises);
  });

  // eth_blockNumber
  logger.info({ msg: "Testing batch eth_blockNumber limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(chain, createSaveFinding("eth_blockNumber"), (i) => {
    const promises = Array.from({ length: i }).map((_, i) => {
      return batchProvider.getBlockNumber();
    });
    return Promise.all(promises);
  });

  logger.info({
    msg: "Testing done for rpc",
    data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl), findings: findings[chain][removeSecretsFromRpcUrl(rpcUrl)] },
  });
}

async function doRpcAllowThisMuch(options: {
  chain: Chain;
  process: (i: number) => Promise<any>;
  batchSize: number;
  maxBatchSize: number;
  lastSuccessfullBatchSize: number;
}) {
  const waitBeforeNextTest = MIN_DELAY_BETWEEN_RPC_CALLS_MS[options.chain];
  try {
    await options.process(options.batchSize);
    logger.debug({ msg: "batch call succeeded", data: options });
    if (waitBeforeNextTest !== "no-limit") {
      await sleep(waitBeforeNextTest);
    }
    // save the findings
    return true;
  } catch (e) {
    logger.info({ msg: "batch limitations found", data: options });
    logger.error(e);
    return false;
  }
}

async function findTheLimit<T>(chain: Chain, saveFinding: (n: number) => void, process: (n: number) => Promise<T>) {
  const maxBatchSize = 500;
  // work our way up to find the limit
  // this avoids overloading the rpc right away (compared to testing the max limit first and working our way down)
  let batchSize = 1;
  let lastSuccessfullBatchSize = batchSize;
  let maxBatchSizeForThisCall = maxBatchSize;
  let infiniteLoopCounter = 1000;
  while (infiniteLoopCounter > 0) {
    infiniteLoopCounter--;
    const ok = await doRpcAllowThisMuch({ batchSize, maxBatchSize: maxBatchSizeForThisCall, lastSuccessfullBatchSize, chain, process });
    // maybe the limit is higher
    if (ok) {
      saveFinding(batchSize);
      if (batchSize + 1 >= maxBatchSizeForThisCall) {
        // we reached the max
        break;
      }

      lastSuccessfullBatchSize = batchSize;
      batchSize = Math.min(batchSize * 2, maxBatchSizeForThisCall);
    } else {
      // the limit is lower
      maxBatchSizeForThisCall = batchSize;
      batchSize = Math.floor((lastSuccessfullBatchSize + maxBatchSizeForThisCall) / 2);

      if (batchSize + 1 >= maxBatchSizeForThisCall) {
        // we are done
        break;
      }
    }
  }

  if (infiniteLoopCounter === 0) {
    throw new ProgrammerError("Infinite loop detected");
  }
  return batchSize;
}
