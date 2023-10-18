import { ethers } from "ethers";
import { cloneDeep, sample, set } from "lodash";
import yargs from "yargs";
import { ImportBehaviour, defaultImportBehaviour } from "../protocol/common/types/import-context";
import { createRpcConfig } from "../protocol/common/utils/rpc-config";
import { Chain, allChainIds } from "../types/chain";
import { RpcCallMethod } from "../types/rpc-config";
import { BeefyVaultV6AbiInterface, ERC20AbiInterface, Multicall3AbiInterface } from "../utils/abi";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import { sleep } from "../utils/async";
import { MULTICALL3_ADDRESS_MAP } from "../utils/config";
import { rootLogger } from "../utils/logger";
import { runMain } from "../utils/process";
import { ProgrammerError } from "../utils/programmer-error";
import { isArchiveNodeNeededError } from "../utils/rpc/archive-node-needed";
import { addSecretsToRpcUrl, removeSecretsFromRpcUrl } from "../utils/rpc/remove-secrets-from-rpc-url";
import {
  MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS,
  MAX_RPC_BATCHING_SIZE,
  MAX_RPC_GETLOGS_SPAN,
  RPC_SOFT_TIMEOUT_MS,
  RpcLimitations,
  defaultLimitations,
  getAllRpcUrlsForChain,
  updateRawLimitations,
} from "../utils/rpc/rpc-limitations";

const logger = rootLogger.child({ module: "script", component: "find-out-rpc-limitations" });

type RpcTests =
  | "eth_call"
  | "eth_getLogs"
  | "eth_getBlockByNumber"
  | "eth_blockNumber"
  | "eth_getTransactionReceipt"
  | "maxGetLogsBlockSpan"
  | "isArchiveNode"
  | "minDelayBetweenCalls"
  | "eth_getLogs_address_batching"
  | "canUseMulticallBlockTimestamp";

const allRpcTests: RpcTests[] = [
  "eth_call",
  "eth_getLogs",
  "eth_getBlockByNumber",
  "eth_blockNumber",
  "eth_getTransactionReceipt",
  "maxGetLogsBlockSpan",
  "isArchiveNode",
  "minDelayBetweenCalls",
  "eth_getLogs_address_batching",
  "canUseMulticallBlockTimestamp",
];

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
    tests: { type: "array", choices: allRpcTests, demand: false, alias: "t", describe: "only run these tests" },
    useDefaultLimitationsIfNotFound: { type: "boolean", demand: false, default: false, alias: "d", describe: "use default limitations if not found" },
  }).argv;
  const rpcFilter = argv.rpc;
  const chainFilter = (argv.chain as Chain | undefined) || null;
  const writeToFile = argv.write;
  const useDefaultLimitationsIfNotFound = argv.useDefaultLimitationsIfNotFound;
  let tests = allRpcTests;
  if (argv.tests) {
    if (writeToFile) {
      throw new ProgrammerError({ msg: "cannot write to file and only run specific tests" });
    }
    tests = argv.tests as RpcTests[];
  }

  if (rpcFilter) {
    if (!chainFilter) {
      throw new Error("If you specify an rpc url, you must also specify a chain");
    }
    findings[chainFilter] = {};
    const behaviour: ImportBehaviour = { ...defaultImportBehaviour, useDefaultLimitationsIfNotFound, forceRpcUrl: addSecretsToRpcUrl(rpcFilter) };
    await testRpcLimits(chainFilter, behaviour, tests);
  } else {
    const allParams = allChainIds
      .filter((chain) => chainFilter === null || chain === chainFilter)
      .map((chain) =>
        getAllRpcUrlsForChain(chain, { ...defaultImportBehaviour, useDefaultLimitationsIfNotFound }).map((rpcUrl) => ({
          chain,
          rpcUrl,
        })),
      )
      .flat();

    for (const { chain } of allParams) {
      findings[chain] = {};
    }

    await Promise.all(
      allParams.map(({ chain, rpcUrl }) =>
        testRpcLimits(chain, { ...defaultImportBehaviour, useDefaultLimitationsIfNotFound, forceRpcUrl: rpcUrl }, tests),
      ),
    );
  }

  if (writeToFile) {
    updateRawLimitations(findings);
  }

  logger.info({ msg: "All done", data: { findings } });
}

runMain(main);

/**
 * We want to find out how many batch calls we can make at once for each call type (eth_call, eth_getLogs, etc)
 *
 * With this information, generate a config file we can use on execution
 **/
async function testRpcLimits(chain: Chain, behaviour: ImportBehaviour, tests: RpcTests[]) {
  const rpcUrl = behaviour.forceRpcUrl;
  if (!rpcUrl) {
    throw new ProgrammerError({ msg: "rpc url must be set" });
  }
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)] = cloneDeep(defaultLimitations);

  logger.info({ msg: "testing rpc", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });

  // ethers timeout can't be caught so we need to test if the rpc responded in a reasonable time
  const { batchProvider, linearProvider, rpcLimitations } = createRpcConfig(chain, behaviour);

  // copy manually set limitations
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].internalTimeoutMs = rpcLimitations.internalTimeoutMs;
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].disableBatching = rpcLimitations.disableBatching;
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].disableRpc = rpcLimitations.disableRpc;
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].weight = rpcLimitations.weight;
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].restrictToMode = rpcLimitations.restrictToMode;
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].minDelayBetweenCalls = rpcLimitations.minDelayBetweenCalls;
  findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].stateChangeReadsOnSameBlock = rpcLimitations.stateChangeReadsOnSameBlock;

  // set the soft timeout, the delay after which we consider the rpc request to have failed
  const rpcSoftTimeout = (rpcLimitations.internalTimeoutMs || RPC_SOFT_TIMEOUT_MS) * 0.5;

  // find out the latest block number
  logger.info({ msg: "fetching latest block number", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
  const latestBlockNumber = await linearProvider.getBlockNumber();

  const createSaveFinding = (key: RpcCallMethod) => (n: number) => {
    if ((findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].methods[key] || 0) < n) {
      findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].methods[key] = n;
      logger.debug({ msg: "Updated findings", data: { rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl), chain, method: key, newValue: n } });
    }
  };

  if (tests.includes("minDelayBetweenCalls")) {
    // for now, only ankr with an API key is no-limit
    const publicRpcUrl = removeSecretsFromRpcUrl(chain, rpcUrl);
    if (publicRpcUrl.includes("ankr.com") && publicRpcUrl.includes("RPC_API_KEY_ANKR")) {
      findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].minDelayBetweenCalls = "no-limit";
    }
  }

  if (tests.includes("isArchiveNode")) {
    // find out if this is an archive node
    // some RPC are in fact clusters of archive and non-archive nodes
    // sometimes we hit a non-archive node and it fails but we can retry to hope we hit an archive node
    let attempts = MAX_RPC_ARCHIVE_NODE_RETRY_ATTEMPTS;
    const wNativeCreationBlock: { [chain in Chain]: number } = {
      arbitrum: 55,
      aurora: 51919680,
      avax: 820,
      base: 1, // weth created on genesis block
      bsc: 149268,
      canto: 85395,
      celo: 2919,
      cronos: 446,
      emerald: 7881,
      ethereum: 4719568,
      fantom: 640717,
      fuse: 7216440,
      gnosis: 11173937,
      harmony: 5481181,
      heco: 404035,
      kava: 393,
      metis: 1400, // couldn't find the exact wtoken creation block
      moonbeam: 171210,
      moonriver: 413534,
      optimism: 1, // weth created on genesis block
      polygon: 4931456,
      zkevm: 129,
      zksync: 9648,
    };
    // disable the baked in retry logic just temporarily
    set(linearProvider, "__disableRetryArchiveNodeErrors", true);
    while (attempts-- > 0) {
      try {
        // we want to do an eth_call
        const balance = await linearProvider.getBalance(getChainWNativeTokenAddress(chain), wNativeCreationBlock[chain] + 1);
        if (balance) {
          findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].isArchiveNode = true;
          break;
        }
      } catch (e) {
        if (isArchiveNodeNeededError(e)) {
          logger.warn({ msg: "Failed to get first block", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl), error: e } });
        } else {
          logger.error({ msg: "Failed, this is not an archive node", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl), error: e } });
          logger.error(e);
          break;
        }
      }
      const waitBeforeNextTest = rpcLimitations.minDelayBetweenCalls;
      if (waitBeforeNextTest !== "no-limit") {
        await sleep(waitBeforeNextTest);
      }
    }
    // disable the baked in retry logic just temporarily
    set(linearProvider, "__disableRetryArchiveNodeErrors", false);
  }

  // maxGetLogsBlockSpan
  let maxBlocksPerQuery = 1;
  if (tests.includes("maxGetLogsBlockSpan")) {
    logger.info({ msg: "Testing batch maxGetLogsBlockSpan limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    await findTheLimit(
      rpcSoftTimeout,
      chain,
      rpcLimitations.minDelayBetweenCalls,
      MAX_RPC_GETLOGS_SPAN,
      (n: number) => {
        if ((findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].maxGetLogsBlockSpan || 0) < n) {
          findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].maxGetLogsBlockSpan = n;
          logger.debug({
            msg: "Updated findings",
            data: { rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl), chain, method: "maxGetLogsBlockSpan", newValue: n },
          });
          // also update current maxBlocksPerQuery
          maxBlocksPerQuery = Math.max(
            maxBlocksPerQuery,
            /*
            reduce the max number of blocks because we are hitting
            a very heavy traffic contract (wgas contract) 
            some rpc (bsc) have a hard time returning large amounts of data
          */
            Math.floor(n / 10),
          );
        }
      },
      (i) => {
        // use an event that never happens so we can get the exact block span limit for this RPC
        // so we target the wgas contract with an event that doesn't exist
        const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), BeefyVaultV6AbiInterface, batchProvider);
        const eventFilter = contract.filters.OwnershipTransferred();
        return contract.queryFilter(eventFilter, latestBlockNumber - i, latestBlockNumber);
      },
    );
  }

  if (tests.includes("eth_getTransactionReceipt")) {
    // eth_getTransactionReceipt
    logger.info({ msg: "testing eth_getTransactionReceipt", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    let someTrxHashes: string[] = [];
    // find a block with transactions
    while (someTrxHashes.length <= 0) {
      someTrxHashes = (await linearProvider.getBlock("latest")).transactions;
    }

    const waitBeforeNextTest = rpcLimitations.minDelayBetweenCalls;
    if (waitBeforeNextTest !== "no-limit") {
      await sleep(waitBeforeNextTest);
    }
    await findTheLimit(
      rpcSoftTimeout,
      chain,
      rpcLimitations.minDelayBetweenCalls,
      MAX_RPC_BATCHING_SIZE,
      createSaveFinding("eth_getTransactionReceipt"),
      async (i) => {
        const promises = Array.from({ length: i }).map((_, i) => {
          const hash = sample(someTrxHashes) as string;
          return batchProvider.getTransactionReceipt(hash);
        });
        return Promise.all(promises);
      },
    );
  }

  if (tests.includes("canUseMulticallBlockTimestamp")) {
    const mcMap = MULTICALL3_ADDRESS_MAP[chain];
    if (!mcMap) {
      // no multicall, we assume it's not possible just to be safe
      findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].canUseMulticallBlockTimestamp = false;
    } else {
      const multicallAddress = mcMap.multicallAddress;
      const multicallContract = new ethers.Contract(multicallAddress, Multicall3AbiInterface, linearProvider);
      // don't request more than 256 blocks to be able to request non-archive rpcs too
      const blockNumber = latestBlockNumber - 100;
      const waitBeforeNextTest = rpcLimitations.minDelayBetweenCalls;
      if (waitBeforeNextTest !== "no-limit") {
        await sleep(waitBeforeNextTest);
      }
      const mcRes = await multicallContract.callStatic.aggregate3(
        [
          {
            allowFailure: false,
            callData: Multicall3AbiInterface.encodeFunctionData("getCurrentBlockTimestamp"),
            target: multicallAddress,
          },
        ],
        { blockTag: blockNumber },
      );

      const mcBlockTimestamp = Multicall3AbiInterface.decodeFunctionResult("getCurrentBlockTimestamp", mcRes[0].returnData)[0] as ethers.BigNumber;
      const mcBlockDate = new Date(mcBlockTimestamp.toNumber() * 1000);

      if (waitBeforeNextTest !== "no-limit") {
        await sleep(waitBeforeNextTest);
      }
      const rpcBlock = await linearProvider.getBlock(blockNumber);
      const rpcBlockTimestamp = rpcBlock.timestamp;
      const rpcBlockDate = new Date(rpcBlockTimestamp * 1000);

      const same = mcBlockDate.getTime() === rpcBlockDate.getTime();

      findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].canUseMulticallBlockTimestamp = same;
    }
  }

  // eth_getLogs
  if (tests.includes("eth_getLogs")) {
    logger.info({ msg: "Testing batch eth_getLogs limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    await findTheLimit(rpcSoftTimeout, chain, rpcLimitations.minDelayBetweenCalls, MAX_RPC_BATCHING_SIZE, createSaveFinding("eth_getLogs"), (i) => {
      const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20AbiInterface, batchProvider);
      const eventFilter = contract.filters.Transfer();
      const promises = Array.from({ length: i }).map((_, i) => {
        const fromBlock = latestBlockNumber - (i + 1) * maxBlocksPerQuery;
        const toBlock = latestBlockNumber - i * maxBlocksPerQuery;
        return contract.queryFilter(eventFilter, fromBlock, toBlock);
      });
      return Promise.all(promises);
    });
  }

  // eth_call
  if (tests.includes("eth_call")) {
    logger.info({ msg: "Testing batch eth_call limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    await findTheLimit(rpcSoftTimeout, chain, rpcLimitations.minDelayBetweenCalls, MAX_RPC_BATCHING_SIZE, createSaveFinding("eth_call"), (i) => {
      const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20AbiInterface, batchProvider);
      const promises = Array.from({ length: i }).map((_, i) => {
        const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
        return contract.balanceOf(getChainWNativeTokenAddress(chain), { blockTag });
      });
      return Promise.all(promises);
    });
  }

  // eth_getBlockByNumber
  if (tests.includes("eth_getBlockByNumber")) {
    logger.info({ msg: "Testing batch eth_getBlockByNumber limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    await findTheLimit(
      rpcSoftTimeout,
      chain,
      rpcLimitations.minDelayBetweenCalls,
      MAX_RPC_BATCHING_SIZE,
      createSaveFinding("eth_getBlockByNumber"),
      (i) => {
        const promises = Array.from({ length: i }).map((_, i) => {
          const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
          return batchProvider.getBlock(blockTag);
        });
        return Promise.all(promises);
      },
    );
  }

  // eth_blockNumber
  if (tests.includes("eth_blockNumber")) {
    logger.info({ msg: "Testing batch eth_blockNumber limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    await findTheLimit(
      rpcSoftTimeout,
      chain,
      rpcLimitations.minDelayBetweenCalls,
      MAX_RPC_BATCHING_SIZE,
      createSaveFinding("eth_blockNumber"),
      (i) => {
        const promises = Array.from({ length: i }).map((_, i) => {
          return batchProvider.getBlockNumber();
        });
        return Promise.all(promises);
      },
    );
  }

  if (tests.includes("eth_getLogs_address_batching")) {
    logger.info({ msg: "Testing eth_getLogs address batching limitations", data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl) } });
    await findTheLimit(
      rpcSoftTimeout,
      chain,
      rpcLimitations.minDelayBetweenCalls,
      MAX_RPC_BATCHING_SIZE,
      (n: number) => {
        if ((findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].maxGetLogsAddressBatchSize || 0) < n) {
          findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].maxGetLogsAddressBatchSize = n;
          logger.debug({
            msg: "Updated findings",
            data: { rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl), chain, method: "maxGetLogsAddressBatchSize", newValue: n },
          });
        }
      },
      (i) => {
        const maxGetLogsBlockSpan = findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)].maxGetLogsBlockSpan || 1000;
        const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20AbiInterface, batchProvider);
        const eventFilter = contract.filters.Transfer();
        return linearProvider.send("eth_getLogs", [
          {
            address: Array.from({ length: i }).map(() => getChainWNativeTokenAddress(chain)),
            fromBlock: ethers.utils.hexValue(latestBlockNumber - maxGetLogsBlockSpan),
            toBlock: ethers.utils.hexValue(latestBlockNumber),
            topics: eventFilter.topics || [],
          },
        ]);
      },
    );
  }

  logger.info({
    msg: "Testing done for rpc",
    data: { chain, rpcUrl: removeSecretsFromRpcUrl(chain, rpcUrl), findings: findings[chain][removeSecretsFromRpcUrl(chain, rpcUrl)] },
  });
}

async function doRpcAllowThisMuch(options: {
  chain: Chain;
  process: (i: number) => Promise<any>;
  batchSize: number;
  maxBatchSize: number;
  minDelayBetweenCalls: number | "no-limit";
  lastSuccessfullBatchSize: number;
}) {
  const waitBeforeNextTest = options.minDelayBetweenCalls;
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

async function findTheLimit<T>(
  softTimeout: number,
  chain: Chain,
  minDelayBetweenCalls: number | "no-limit",
  maxBatchSizeForThisCall: number,
  saveFinding: (n: number) => void,
  process: (n: number) => Promise<T>,
) {
  // work our way up to find the limit
  // this avoids overloading the rpc right away (compared to testing the max limit first and working our way down)
  let batchSize = 1;
  let lastSuccessfullBatchSize = batchSize;
  let infiniteLoopCounter = 1000;
  while (infiniteLoopCounter > 0) {
    infiniteLoopCounter--;
    try {
      const start = Date.now();
      const callOk = await doRpcAllowThisMuch({
        batchSize,
        maxBatchSize: maxBatchSizeForThisCall,
        minDelayBetweenCalls,
        lastSuccessfullBatchSize,
        chain,
        process,
      });
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
