import { ethers } from "ethers";
import * as fs from "fs";
import { cloneDeep, sample, set } from "lodash";
import yargs from "yargs";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { allChainIds, Chain } from "../types/chain";
import { RpcCallMethod } from "../types/rpc-config";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import { sleep } from "../utils/async";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, CONFIG_DIRECTORY, MIN_DELAY_BETWEEN_RPC_CALLS_MS, RPC_URLS } from "../utils/config";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { ProgrammerError } from "../utils/programmer-error";
import { isArchiveNodeNeededError } from "../utils/rpc/archive-node-needed";
import { removeSecretsFromRpcUrl } from "../utils/rpc/remove-secrets-from-rpc-url";
import { defaultLimitations, MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS, MAX_RPC_BATCH_SIZE, RpcLimitations } from "../utils/rpc/rpc-limitations";

const logger = rootLogger.child({ module: "script", component: "find-out-rpc-limitations" });

const findings = {} as {
  [chain in Chain]: {
    [rpcUrl: string]: RpcLimitations;
  };
};

async function main() {
  const argv = await yargs.usage("$0 <cmd> [args]").options({
    rpc: { type: "string", demand: false, alias: "r", describe: "only use this rpc url" },
    chain: { type: "string", demand: false, alias: "c", describe: "only use this chain" },
    write: { type: "boolean", demand: false, alias: "w", default: false, describe: "write findings to file" },
  }).argv;
  const rpcFilter = argv.rpc;
  const chainFilter = (argv.chain as Chain | undefined) || null;
  const writeToFile = argv.write;

  if (rpcFilter) {
    if (!chainFilter) {
      throw new Error("If you specify an rpc url, you must also specify a chain");
    }
    findings[chainFilter] = {};
    await testRpcLimits(chainFilter, rpcFilter, writeToFile);
  } else {
    const allParams = allChainIds
      .filter((chain) => chainFilter === null || chain === chainFilter)
      .map((chain) => RPC_URLS[chain].map((rpcUrl) => ({ chain, rpcUrl })))
      .flat();

    for (const { chain } of allParams) {
      findings[chain] = {};
    }

    await Promise.all(allParams.map(({ chain, rpcUrl }) => testRpcLimits(chain, rpcUrl, writeToFile)));
  }

  logger.info({ msg: "All done", data: { findings } });
}

runMain(main);

/**
 * We want to find out how many batch calls we can make at once for each call type (eth_call, eth_getLogs, etc)
 *
 * With this information, generate a config file we can use on execution
 **/
async function testRpcLimits(chain: Chain, rpcUrl: string, writeToFile: boolean) {
  findings[chain][removeSecretsFromRpcUrl(rpcUrl)] = cloneDeep(defaultLimitations);

  logger.info({ msg: "testing rpc", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });

  // ethers timeout can't be caught so we need to test if the rpc responded in a reasonable time
  const softTimeout = 5_000;
  const rpcOptions: ethers.utils.ConnectionInfo = { url: rpcUrl, timeout: undefined };
  const { batchProvider, linearProvider } = createRpcConfig(chain, rpcOptions);

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
    if ((findings[chain][removeSecretsFromRpcUrl(rpcUrl)].methods[key] || 0) < n) {
      findings[chain][removeSecretsFromRpcUrl(rpcUrl)].methods[key] = n;
      logger.debug({ msg: "Updated findings", data: { rpcUrl: removeSecretsFromRpcUrl(rpcUrl), chain, method: key, newValue: n } });
    }
  };

  // find out if this is an archive node
  // some RPC are in fact clusters of archive and non-archive nodes
  // sometimes we hit a non-archive node and it fails but we can retry to hope we hit an archive node
  let attempts = MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS;
  const wNativeCreationBlock: { [chain in Chain]: number } = {
    arbitrum: 55,
    aurora: 51919680,
    avax: 820,
    bsc: 149268,
    celo: 2919,
    cronos: 446,
    emerald: 7881,
    fantom: 640717,
    fuse: 7216440,
    harmony: 5481181,
    heco: 404035,
    kava: 393,
    metis: 1400, // couldn't find the exact wtoken creation block
    moonbeam: 171210,
    moonriver: 413534,
    // not sure if this is correct, explorer shows a weird block
    // https://optimistic.etherscan.io/address/0x4200000000000000000000000000000000000006
    // https://optimistic.etherscan.io/tx/GENESIS_4200000000000000000000000000000000000006 -> 404
    optimism: 1,
    polygon: 4931456,
    syscoin: 1523,
  };
  // disable the baked in retry logic just temporarily
  set(linearProvider, "__disableRetryArchiveNodeErrors", true);
  while (attempts-- > 0) {
    try {
      const balance = await linearProvider.getBalance(getChainWNativeTokenAddress(chain), wNativeCreationBlock[chain] + 1);
      if (balance) {
        findings[chain][removeSecretsFromRpcUrl(rpcUrl)].isArchiveNode = true;
        break;
      }
    } catch (e) {
      if (isArchiveNodeNeededError(e)) {
        logger.warn({ msg: "Failed to get first block", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl), error: e } });
      } else {
        logger.error({ msg: "Failed, this is not an archive node", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl), error: e } });
        logger.error(e);
        break;
      }
    }
    const waitBeforeNextTest = MIN_DELAY_BETWEEN_RPC_CALLS_MS[chain];
    if (waitBeforeNextTest !== "no-limit") {
      await sleep(waitBeforeNextTest);
    }
  }
  // disable the baked in retry logic just temporarily
  set(linearProvider, "__disableRetryArchiveNodeErrors", false);

  // eth_getTransactionReceipt
  logger.info({ msg: "testing eth_getTransactionReceipt", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  let someTrxHashes: string[] = [];
  // find a block with transactions
  while (someTrxHashes.length <= 0) {
    someTrxHashes = (await linearProvider.getBlock("latest")).transactions;
  }

  const waitBeforeNextTest = MIN_DELAY_BETWEEN_RPC_CALLS_MS[chain];
  if (waitBeforeNextTest !== "no-limit") {
    await sleep(waitBeforeNextTest);
  }
  await findTheLimit(softTimeout, chain, createSaveFinding("eth_getTransactionReceipt"), async (i) => {
    const promises = Array.from({ length: i }).map((_, i) => {
      const hash = sample(someTrxHashes) as string;
      return batchProvider.getTransactionReceipt(hash);
    });
    return Promise.all(promises);
  });

  // eth_getLogs
  logger.info({ msg: "Testing batch eth_getLogs limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(softTimeout, chain, createSaveFinding("eth_getLogs"), (i) => {
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
  await findTheLimit(softTimeout, chain, createSaveFinding("eth_call"), (i) => {
    const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20Abi, batchProvider);
    const promises = Array.from({ length: i }).map((_, i) => {
      const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
      return contract.balanceOf(getChainWNativeTokenAddress(chain), { blockTag });
    });
    return Promise.all(promises);
  });

  // eth_getBlockByNumber
  logger.info({ msg: "Testing batch eth_getBlockByNumber limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(softTimeout, chain, createSaveFinding("eth_getBlockByNumber"), (i) => {
    const promises = Array.from({ length: i }).map((_, i) => {
      const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
      return batchProvider.getBlock(blockTag);
    });
    return Promise.all(promises);
  });

  // eth_blockNumber
  logger.info({ msg: "Testing batch eth_blockNumber limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(rpcUrl) } });
  await findTheLimit(softTimeout, chain, createSaveFinding("eth_blockNumber"), (i) => {
    const promises = Array.from({ length: i }).map((_, i) => {
      return batchProvider.getBlockNumber();
    });
    return Promise.all(promises);
  });

  if (writeToFile) {
    const findingsFile = CONFIG_DIRECTORY + "/rpc-limitations.json";
    const originalContent = JSON.parse(fs.readFileSync(findingsFile, "utf-8"));
    fs.writeFileSync(findingsFile, JSON.stringify(Object.assign({}, originalContent, findings), null, 2));
  }
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

async function findTheLimit<T>(softTimeout: number, chain: Chain, saveFinding: (n: number) => void, process: (n: number) => Promise<T>) {
  // work our way up to find the limit
  // this avoids overloading the rpc right away (compared to testing the max limit first and working our way down)
  let batchSize = 1;
  let lastSuccessfullBatchSize = batchSize;
  let maxBatchSizeForThisCall = MAX_RPC_BATCH_SIZE;
  let infiniteLoopCounter = 1000;
  while (infiniteLoopCounter > 0) {
    infiniteLoopCounter--;
    try {
      const start = Date.now();
      const callOk = await doRpcAllowThisMuch({ batchSize, maxBatchSize: maxBatchSizeForThisCall, lastSuccessfullBatchSize, chain, process });
      const end = Date.now();
      const time = end - start;

      // if the call was not fast enough, we consider the limit to be lower
      const timeoutOk = time < softTimeout;

      // maybe the limit is higher
      if (callOk && timeoutOk) {
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
    } catch (e) {
      logger.error({
        msg: "Error while testing batch limitations",
        data: { chain, batchSize, maxBatchSizeForThisCall, lastSuccessfullBatchSize, e },
      });
      logger.error(e);
      throw e;
    }
  }

  if (infiniteLoopCounter === 0) {
    throw new ProgrammerError("Infinite loop detected");
  }
  return batchSize;
}
