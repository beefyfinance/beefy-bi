import { ethers } from "ethers";
import { allChainIds, Chain } from "../types/chain";
import { CHAIN_RPC_MAX_QUERY_BLOCKS, RPC_URLS } from "../utils/config";
import { getChainWNativeTokenAddress } from "../utils/addressbook";
import { runMain } from "../utils/process";
import ERC20Abi from "../../data/interfaces/standard/ERC20.json";
import { rootLogger } from "../utils/logger";
import { addDebugLogsToProvider } from "../utils/ethers";
import { RpcCallMethod } from "../types/rpc-config";
import yargs from "yargs";
import { sleep } from "../utils/async";

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
    for (const chain of allChainIds) {
      if (chainFilter && chain !== chainFilter) {
        continue;
      }

      findings[chain] = {};

      for (const rpcUrl of RPC_URLS[chain]) {
        await testRpcLimits(chain, rpcUrl);
      }
    }
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
  findings[chain][rpcUrl] = {
    eth_getLogs: null,
    eth_call: null,
    eth_getBlockByNumber: null,
    eth_blockNumber: null,
  };

  logger.info({ msg: "testing rpc", data: { chain, rpcUrl } });

  const rpcOptions: ethers.utils.ConnectionInfo = { url: rpcUrl, timeout: 30_000 };
  const batchProvider = new ethers.providers.JsonRpcBatchProvider(rpcOptions);
  const linearProvider = new ethers.providers.JsonRpcProvider(rpcOptions);
  addDebugLogsToProvider(batchProvider);
  addDebugLogsToProvider(linearProvider);

  // find out the latest block number
  logger.info({ msg: "fetching latest block number" });
  const latestBlockNumber = await linearProvider.getBlockNumber();
  const maxBlocksPerQuery = CHAIN_RPC_MAX_QUERY_BLOCKS[chain];

  const createSaveFinding = (key: RpcCallMethod) => (n: number) => {
    if ((findings[chain][rpcUrl][key] || 0) < n) {
      findings[chain][rpcUrl][key] = n;
    }
    console.dir(findings, { depth: null });
  };

  // eth_getLogs
  logger.info({ msg: "Testing batch eth_getLogs limitations" });
  await findTheLimit(createSaveFinding("eth_getLogs"), (i) => {
    const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20Abi, batchProvider);
    const eventFilter = contract.filters.Transfer();
    const promises = Array.from({ length: i }).map((_, i) => {
      const fromBlock = latestBlockNumber - i * maxBlocksPerQuery;
      const toBlock = latestBlockNumber - (i + 1) * maxBlocksPerQuery;
      return contract.queryFilter(eventFilter, fromBlock, toBlock);
    });
    return Promise.all(promises);
  });

  // eth_call
  logger.info({ msg: "Testing batch eth_call limitations" });
  await findTheLimit(createSaveFinding("eth_call"), (i) => {
    const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20Abi, batchProvider);
    const promises = Array.from({ length: i }).map((_, i) => {
      const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
      return contract.balanceOf(getChainWNativeTokenAddress(chain), { blockTag });
    });
    return Promise.all(promises);
  });

  // eth_getBlockByNumber
  logger.info({ msg: "Testing batch eth_getBlockByNumber limitations" });
  await findTheLimit(createSaveFinding("eth_getBlockByNumber"), (i) => {
    const contract = new ethers.Contract(getChainWNativeTokenAddress(chain), ERC20Abi, batchProvider);
    const promises = Array.from({ length: i }).map((_, i) => {
      const blockTag = latestBlockNumber - i * maxBlocksPerQuery;
      return contract.getBlock(blockTag);
    });
    return Promise.all(promises);
  });

  // eth_blockNumber
  logger.info({ msg: "Testing batch eth_blockNumber limitations" });
  await findTheLimit(createSaveFinding("eth_blockNumber"), (i) => {
    const promises = Array.from({ length: i }).map((_, i) => {
      return batchProvider.getBlockNumber();
    });
    return Promise.all(promises);
  });

  logger.info({ msg: "Testing done for rpc", data: { chain, rpcUrl, findings: findings[chain][rpcUrl] } });
}

async function findTheLimit<T>(saveFinding: (n: number) => void, process: (n: number) => Promise<T>) {
  const maxBatchSize = 500;
  const waitBeforeNextTest = 1000;
  async function doRpcAllowThisMuch(batchSize: number) {
    try {
      await process(batchSize);
      logger.debug({ msg: "batch call succeeded", data: { batchSize } });
      saveFinding(batchSize);
      await sleep(waitBeforeNextTest);
      // save the findings
      return true;
    } catch (e) {
      logger.info({ msg: "batch limitations found", data: { batchSize } });
      logger.error(e);
      return false;
    }
  }

  // test 1, just to make sure we can do this
  await doRpcAllowThisMuch(1);

  // first, try the max, we never know
  if (await doRpcAllowThisMuch(maxBatchSize)) {
    return maxBatchSize;
  }

  // then work by halves
  let min = 1;
  let max = maxBatchSize;
  while (max - min > 1) {
    const mid = Math.floor((min + max) / 2);
    if (await doRpcAllowThisMuch(mid)) {
      min = mid;
    } else {
      max = mid;
    }
  }
  return min;
}
