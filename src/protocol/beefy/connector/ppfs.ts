import { Chain } from "../../../types/chain";
import BeefyVaultV6Abi from "../../../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { ethers } from "ethers";
import axios from "axios";
import { sortBy } from "lodash";
import { rootLogger } from "../../../utils/logger";
import * as Rx from "rxjs";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../lib/rpc/archive-node-needed";
import { batchQueryGroup$ } from "../../../utils/rxjs/utils/batch-query-group";
import Decimal from "decimal.js";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

export function fetchBeefyPPFS$<TObj, TParams extends { contractAddress: string; blockNumber: number }, TRes>(options: {
  provider: ethers.providers.JsonRpcProvider;
  chain: Chain;
  getQueryParams: (obj: TObj) => TParams;
  formatOutput: (obj: TObj, ppfs: Decimal) => TRes;
}): Rx.OperatorFunction<TObj, TRes> {
  return batchQueryGroup$({
    bufferCount: 200,
    // there should be only one query for each group since we batch by owner and contract address
    toQueryObj: (obj: TObj[]) => options.getQueryParams(obj[0]),
    // we process all transfers by individual user
    getBatchKey: (param: TObj) => {
      const { contractAddress, ownerAddress, blockNumber } = options.getQueryParams(param);
      return `${contractAddress}-${ownerAddress}-${blockNumber}`;
    },
    // do the actual processing
    processBatch: async (params: TParams[]) => {
      const balancePromises: Promise<Decimal>[] = [];
      for (const param of params) {
        const valueMultiplier = new Decimal(10).pow(-param.decimals);
        const contract = new ethers.Contract(param.contractAddress, ERC20Abi, options.provider);

        // aurora RPC return the state before the transaction is applied
        let blockTag = param.blockNumber;
        if (options.chain === "aurora") {
          blockTag = param.blockNumber + 1;
        }

        const balancePromise = contract
          .balanceOf(param.ownerAddress, { blockTag })
          .then((balance: ethers.BigNumber) => valueMultiplier.mul(balance.toString() ?? "0"));
        balancePromises.push(balancePromise);
      }
      return Promise.all(balancePromises);
    },
    formatOutput: options.formatOutput,
  });
}

async function fetchBeefyPPFS(
  provider: ethers.providers.JsonRpcBatchProvider,
  chain: Chain,
  contractAddresses: string[],
  blockNumbers: number[],
): Promise<ethers.BigNumber[]> {
  logger.debug({
    msg: "Batch fetching PPFS",
    data: {
      chain,
      contractAddresses,
      from: blockNumbers[0],
      to: blockNumbers[blockNumbers.length - 1],
      length: blockNumbers.length,
    },
  });

  let ppfsPromises: Promise<[ethers.BigNumber]>[] = [];

  // it looks like ethers doesn't yet support harmony's special format or smth
  // same for heco
  if (chain === "harmony" || chain === "heco") {
    for (const contractAddress of contractAddresses) {
      const contract = new ethers.Contract(contractAddress, BeefyVaultV6Abi, provider);
      const ppfsPromise = await fetchBeefyPPFSWithManualRPCCall(provider, chain, contractAddress, blockNumbers);
      ppfsPromises = ppfsPromises.concat(ppfsPromise);
    }
  } else {
    // fetch all ppfs in one go, this will batch calls using jsonrpc batching
    for (const contractAddress of contractAddresses) {
      const contract = new ethers.Contract(contractAddress, BeefyVaultV6Abi, provider);
      for (const blockNumber of blockNumbers) {
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
  const ppfss: ethers.BigNumber[] = [];
  for (const ppfsRes of ppfsResults) {
    if (ppfsRes.status === "fulfilled") {
      ppfss.push(ppfsRes.value[0]);
    } else {
      // sometimes, we get this error: "execution reverted: SafeMath: division by zero"
      // this means that the totalSupply is 0 so we set ppfs to zero
      if (ppfsRes.reason.message.includes("SafeMath: division by zero")) {
        ppfss.push(ethers.BigNumber.from("0"));
      } else {
        // otherwise, we throw the error
        throw ppfsRes.reason;
      }
    }
  }
  return ppfss;
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
