import { Chain } from "../../../types/chain";
import * as Rx from "rxjs";
import BeefyVaultV6Abi from "../../../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { BigNumber, ethers } from "ethers";
import { rootLogger } from "../../../utils/logger2";
import axios from "axios";
import { flatten, sortBy } from "lodash";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../../lib/rpc/archive-node-needed";
import { batchQueryGroup } from "../../../utils/rxjs/utils/batch-query-group";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

interface BeefyPPFSCallParams {
  vaultDecimals: number;
  underlyingDecimals: number;
  vaultAddress: string;
  blockNumbers: number[];
}

export function mapBeefyPPFS<TObj, TKey extends string, TParams extends BeefyPPFSCallParams>(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  getParams: (obj: TObj) => TParams,
  toKey: TKey,
): Rx.OperatorFunction<TObj[], (TObj & { [key in TKey]: BigNumber })[]> {
  // we want to make a query for all requested block numbers of this contract
  const toQueryObj = (objs: TObj[]): TParams => {
    const params = objs.map(getParams);
    return { ...params[0], blockNumbers: flatten(params.map((p) => p.blockNumbers)) };
  };
  const getKeyFromObj = (obj: TObj) => getKeyFromParams(getParams(obj));
  const getKeyFromParams = ({ vaultAddress }: TParams) => {
    return `${vaultAddress.toLocaleLowerCase()}`;
  };

  const process = async (params: TParams[]) => fetchBeefyPPFS(provider, chain, params);

  return batchQueryGroup(toQueryObj, getKeyFromObj, process, toKey);
}

export async function fetchBeefyPPFS(
  provider: ethers.providers.JsonRpcProvider,
  chain: Chain,
  contractCalls: BeefyPPFSCallParams[],
): Promise<ethers.BigNumber[]> {
  logger.debug({
    msg: "Batch fetching PPFS",
    data: {
      chain,
      count: contractCalls.length,
    },
  });

  let ppfsPromises: Promise<[ethers.BigNumber]>[] = [];

  // it looks like ethers doesn't yet support harmony's special format or smth
  // same for heco
  if (chain === "harmony" || chain === "heco") {
    for (const contractCall of contractCalls) {
      const ppfsPromise = await fetchBeefyPPFSWithManualRPCCall(
        provider,
        chain,
        contractCall.vaultAddress,
        contractCall.blockNumbers,
      );
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
