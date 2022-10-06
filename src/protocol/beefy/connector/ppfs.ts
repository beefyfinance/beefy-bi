import { Chain } from "../../../types/chain";
import BeefyVaultV6Abi from "../../../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { ethers } from "ethers";
import axios from "axios";
import { sortBy } from "lodash";
import { rootLogger } from "../../../utils/logger";
import * as Rx from "rxjs";
import { RpcConfig } from "../../../types/rpc-config";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../lib/rpc/archive-node-needed";
import { BatchStreamConfig, batchRpcCalls$ } from "../../common/utils/batch-rpc-calls";
import Decimal from "decimal.js";
import { DbProduct } from "../../common/loader/product";
import { ErrorEmitter, ProductImportQuery } from "../../common/types/product-query";
import { getRpcRetryConfig } from "../../common/utils/rpc-retry-config";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

interface BeefyPPFSCallParams {
  vaultDecimals: number;
  underlyingDecimals: number;
  vaultAddress: string;
  blockNumbers: number[];
}

export function fetchBeefyPPFS$<
  TProduct extends DbProduct,
  TObj extends ProductImportQuery<TProduct>,
  TParams extends BeefyPPFSCallParams,
  TRes extends ProductImportQuery<TProduct>,
>(options: {
  rpcConfig: RpcConfig;
  chain: Chain;
  getPPFSCallParams: (obj: TObj) => TParams;
  emitErrors: ErrorEmitter;
  streamConfig: BatchStreamConfig;
  formatOutput: (obj: TObj, ppfss: Decimal[]) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  const logInfos = { msg: "Fetching Beefy PPFS", data: { chain: options.chain } };
  return batchRpcCalls$({
    logInfos,
    rpcConfig: options.rpcConfig,
    rpcCallsPerInputObj: {
      eth_call: 1,
      eth_blockNumber: 0,
      eth_getBlockByNumber: 0,
      eth_getLogs: 0,
    },
    emitErrors: options.emitErrors,
    streamConfig: options.streamConfig,
    getQuery: options.getPPFSCallParams,
    processBatch: (provider, params) => fetchBeefyVaultPPFS(provider, options.chain, params),
    formatOutput: options.formatOutput,
  });
}

export async function fetchBeefyVaultPPFS(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: BeefyPPFSCallParams[],
): Promise<Decimal[][]> {
  // short circuit if no calls
  if (contractCalls.length === 0) {
    return [];
  }

  logger.debug({
    msg: "Batch fetching PPFS",
    data: {
      chain,
      contractCalls: contractCalls.map((c) => c.blockNumbers).flat().length,
      contractCount: contractCalls.length,
    },
  });

  let ppfsPromises: Promise<[ethers.BigNumber]>[] = [];

  // it looks like ethers doesn't yet support harmony's special format or smth
  // same for heco
  if (chain === "harmony" || chain === "heco") {
    for (const contractCall of contractCalls) {
      const ppfsPromise = await fetchBeefyPPFSWithManualRPCCall(provider, chain, contractCall.vaultAddress, contractCall.blockNumbers);
      ppfsPromises = ppfsPromises.concat(ppfsPromise);
    }
  } else {
    // fetch all ppfs in one go, this will batch calls using jsonrpc batching
    for (const contractCall of contractCalls) {
      const contract = new ethers.Contract(contractCall.vaultAddress, BeefyVaultV6Abi, provider);
      for (const blockNumber of contractCall.blockNumbers) {
        const ppfsPromise = contract.functions.getPricePerFullShare({
          // a block tag to simulate the execution at, which can be used for hypothetical historic analysis;
          // note that many backends do not support this, or may require paid plans to access as the node
          // database storage and processing requirements are much higher
          blockTag: blockNumber,
        });
        ppfsPromises.push(ppfsPromise);
      }
    }
  }

  const ppfsResults = await Promise.allSettled(ppfsPromises);
  const rates: Decimal[][] = [];
  let resultIdx = 0;
  for (const contractCall of contractCalls) {
    const contractShareRates: Decimal[] = [];
    for (const _ of contractCall.blockNumbers) {
      const ppfsRes = ppfsResults[resultIdx];
      resultIdx++;

      let ppfs: ethers.BigNumber;
      if (ppfsRes.status === "fulfilled") {
        ppfs = ppfsRes.value[0];
      } else {
        // sometimes, we get this error: "execution reverted: SafeMath: division by zero"
        // this means that the totalSupply is 0 so we set ppfs to zero
        if (ppfsRes.reason.message.includes("SafeMath: division by zero")) {
          ppfs = ethers.BigNumber.from("0");
        } else {
          // otherwise, we throw the error
          throw ppfsRes.reason;
        }
      }

      const vaultShareRate = ppfsToVaultSharesRate(contractCall.vaultDecimals, contractCall.underlyingDecimals, ppfs);
      contractShareRates.push(vaultShareRate);
    }

    rates.push(contractShareRates);
  }

  return rates;
}

// takes ppfs and compute the actual rate which can be directly multiplied by the vault balance
// this is derived from mooAmountToOracleAmount in beefy-v2 repo
export function ppfsToVaultSharesRate(mooTokenDecimals: number, depositTokenDecimals: number, ppfs: ethers.BigNumber) {
  const mooTokenAmount = new Decimal("1.0");

  // go to chain representation
  const mooChainAmount = mooTokenAmount.mul(new Decimal(10).pow(mooTokenDecimals)).toDecimalPlaces(0);

  // convert to oracle amount in chain representation
  const oracleChainAmount = mooChainAmount.mul(new Decimal(ppfs.toString()));

  // go to math representation
  // but we can't return a number with more precision than the oracle precision
  const oracleAmount = oracleChainAmount.div(new Decimal(10).pow(mooTokenDecimals + depositTokenDecimals)).toDecimalPlaces(mooTokenDecimals);

  return oracleAmount;
}

/**
 * I don't know why this is needed but seems like ethers.js is not doing the right rpc call
 */
async function fetchBeefyPPFSWithManualRPCCall(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractAddress: string,
  blockNumbers: number[],
): Promise<Promise<[ethers.BigNumber]>[]> {
  // short circuit if no calls
  if (blockNumbers.length === 0) {
    return [];
  }

  const url = provider.connection.url;

  // get the function call hash
  const abi = ["function getPricePerFullShare()"];
  const iface = new ethers.utils.Interface(abi);
  const callData = iface.encodeFunctionData("getPricePerFullShare");

  // somehow block tag has to be hex encoded for heco
  const batchParams = blockNumbers.map((blockNumber, idx) => ({
    method: "eth_call",
    params: [
      {
        from: null,
        to: contractAddress,
        data: callData,
      },
      ethers.utils.hexValue(blockNumber),
    ],
    id: idx,
    jsonrpc: "2.0",
  }));

  type BatchResItem =
    | {
        jsonrpc: "2.0";
        id: number;
        result: string;
      }
    | {
        jsonrpc: "2.0";
        id: number;
        error: string;
      };
  const results = await axios.post<BatchResItem[]>(url, batchParams);

  return sortBy(results.data, (res) => res.id).map((res) => {
    if (isErrorDueToMissingDataFromNode(res)) {
      throw new ArchiveNodeNeededError(chain, res);
    } else if ("error" in res) {
      throw new Error("Error in fetching PPFS: " + JSON.stringify(res));
    }
    const ppfs = ethers.utils.defaultAbiCoder.decode(["uint256"], res.result) as any as [ethers.BigNumber];
    return Promise.resolve(ppfs);
  });
}
