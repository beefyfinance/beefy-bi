import { Chain } from "../../types/chain";
import BeefyVaultV6Abi from "../../../data/interfaces/beefy/BeefyVaultV6/BeefyVaultV6.json";
import { ethers } from "ethers";
import { ArchiveNodeNeededError, isErrorDueToMissingDataFromNode } from "../../lib/shared-resources/shared-rpc";
import { rootLogger } from "../../utils/logger2";
import axios from "axios";
import { sortBy } from "lodash";

const logger = rootLogger.child({ module: "beefy", component: "ppfs" });

export async function fetchBeefyPPFS(
  chain: Chain,
  contractAddress: string,
  provider: ethers.providers.JsonRpcProvider,
  blockNumbers: number[]
): Promise<ethers.BigNumber[]> {
  const contract = new ethers.Contract(contractAddress, BeefyVaultV6Abi, provider);

  logger.debug({
    msg: "Batch fetching PPFS",
    data: {
      chain,
      contractAddress,
      from: blockNumbers[0],
      to: blockNumbers[blockNumbers.length - 1],
      length: blockNumbers.length,
    },
  });

  // it looks like ethers doesn't yet support harmony's special format or smth
  // same for heco
  const ppfsPromises =
    chain === "harmony" || chain === "heco"
      ? await fetchBeefyPPFSWithManualRPCCall(provider, chain, contractAddress, blockNumbers)
      : blockNumbers.map((blockNumber) => {
          return contract.functions.getPricePerFullShare({
            // a block tag to simulate the execution at, which can be used for hypothetical historic analysis;
            // note that many backends do not support this, or may require paid plans to access as the node
            // database storage and processing requirements are much higher
            blockTag: blockNumber,
          }) as Promise<[ethers.BigNumber]>;
        });

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
  blockNumbers: number[]
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
